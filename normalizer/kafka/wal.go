package kafka

import (
	"bufio"
	"encoding/json"
	"market-normalizer/constants"
	"os"
	"shared/logger"
	"sync"
	"time"
)

// simple log file which is filled up in the event of failed kafka produce (kafka down, network partition, auth error)
// fill this log file up and later replay from it to downstream. maintain dedupe in aggregator too.
type WALEntry struct {
	Ts    int64                      `json:"ts"`
	Topic string                     `json:"topic"`
	Key   []byte                     `json:"key"`
	Value []byte                     `json:"value"`
	Msg   *constants.PipelineMessage `json:"msg"`
}

type WAL struct {
	mu                 sync.Mutex
	file               *os.File
	writer             *bufio.Writer
	path               string
	maxEntries         int64
	entryCount         int64
	threshold          float64
	cooldownTimeMillis int
	topics             []string
}

var (
	Wal                     *WAL
	ReplayFailureHookRecord int
	ReplayFailureHook       func(int) error
	ReplayStartedHook       func()
)

func NewWAL(cfg *constants.KafkaConfig) (*WAL, error) {
	f, err := os.OpenFile(cfg.WALPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	Wal = &WAL{
		file:               f,
		path:               cfg.WALPath,
		maxEntries:         cfg.WALMaxEntries,
		writer:             bufio.NewWriter(f),
		threshold:          cfg.WALBackpressureThreshold,
		cooldownTimeMillis: cfg.WALCooldownTimeMillis,
		topics:             cfg.Topics,
	}

	return Wal, nil
}

// this takes the message, creates an entry and appends it to the file
func (w *WAL) Append(topic string, msg *constants.PipelineMessage, key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	logger.Log.Info("Ready to append record to WAL", "entryCount", w.entryCount+1, "topic", topic, "key", string(key), "exchange", msg.Exchange, "channel", msg.Channel, "symbol", msg.Symbol)

	if w.entryCount >= w.maxEntries {
		return logger.LogAndWrap("WAL entry count >= Max entries", nil, "entryCount", w.entryCount, "maxEntries", w.maxEntries)
	}

	// backpressure - pause partition fetch for x millis
	ratio := float64(w.entryCount) / float64(w.maxEntries)
	if ratio >= w.threshold {
		Client.PauseFetchTopics(w.topics...)
		time.AfterFunc(time.Duration(w.cooldownTimeMillis)*time.Millisecond, func() {
			Client.ResumeFetchTopics(w.topics...)
		})
	}

	entry := WALEntry{
		Ts:    time.Now().UnixMilli(),
		Topic: topic,
		Key:   key,
		Value: value,
		Msg:   msg,
	}

	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	if _, err := w.writer.Write(append(b, '\n')); err != nil {
		return err
	}

	w.entryCount++
	return w.writer.Flush()
}

// to replay the entries from wal file upon circuit closed
func (w *WAL) Replay(handler func(idx int, entry WALEntry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if ReplayStartedHook != nil {
		ReplayStartedHook()
	}

	logger.Log.Debug("Inside WAL Replay(entry) method")

	w.writer.Flush()
	w.file.Close()

	f, err := os.Open(w.path)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(f)
	var entries []WALEntry

	for scanner.Scan() {
		var e WALEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		logger.Log.Info("Read entry from WAL", "entry", e)
		entries = append(entries, e)
	}

	entriesProcessed := 0
	for i, entry := range entries {

		if err := handler(i, entry); err != nil {
			logger.Log.Error("Error occurred in WAL Replay", "err", err)
			// rewrite wal with the new entries starting from where error came
			remaining := entries[entriesProcessed:]
			if rewriteErr := w.RewriteWAL(remaining); rewriteErr != nil {
				return logger.LogAndWrap("Error when rewriting WAL", rewriteErr)
			}
			return err
		}
		entriesProcessed++
	}

	// in the case we got no errors during replay, create a new empty file
	return w.RewriteWAL(nil)
}

// rewrite the failed entries if exists into this file
// in the case my wal replay failed in the middle, i dont want to replay already processed entries
func (w *WAL) RewriteWAL(entries []WALEntry) error {
	logger.Log.Info("Starting Rewrite WAL")

	w.Close()

	f, err := os.OpenFile(w.path, os.O_TRUNC|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(f)

	for _, entry := range entries {
		b, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		writer.Write(append(b, '\n'))
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	w.file = f
	w.writer = writer
	w.entryCount = int64(len(entries))

	logger.Log.Info("Exiting Rewrite WAL")

	return nil
}

func (w *WAL) Close() error {
	w.writer.Flush()
	return w.file.Close()
}
