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
	Ts    int64  `json:"ts"`
	Topic string `json:"topic"`
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
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
	wal *WAL
)

func NewWAL(cfg *constants.KafkaConfig) (*WAL, error) {
	f, err := os.OpenFile(cfg.WALPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	wal = &WAL{
		file:               f,
		path:               cfg.WALPath,
		maxEntries:         cfg.WALMaxEntries,
		writer:             bufio.NewWriter(f),
		threshold:          cfg.WALBackpressureThreshold,
		cooldownTimeMillis: cfg.WALCooldownTimeMillis,
		topics:             cfg.Topics,
	}

	return wal, nil
}

// this takes the message, creates an entry and appends it to the file
func (w *WAL) Append(topic string, key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

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
		Ts:    time.Now().UnixNano(),
		Topic: topic,
		Key:   key,
		Value: value,
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
func (w *WAL) Replay(handler func(entry WALEntry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.writer.Flush()
	w.file.Close()

	f, err := os.Open(w.path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var entries []WALEntry

	for scanner.Scan() {
		var e WALEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		entries = append(entries, e)
	}

	entriesProcessed := 0
	for _, entry := range entries {
		if err := handler(entry); err != nil {
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

// create a .tmp file, write the entries into this file and rename to my original file
// in the case my wal replay failed in the middle, i dont want to replay already processed entries
func (w *WAL) RewriteWAL(entries []WALEntry) error {
	tmpPath := w.path + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer tmpFile.Close()

	writer := bufio.NewWriter(tmpFile)
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

	if err := os.Rename(tmpPath, w.path); err != nil {
		return err
	}

	w.file, err = os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.writer = bufio.NewWriter(w.file)
	w.entryCount = int64(len(entries))

	return nil
}

func (w *WAL) Close() error {
	w.writer.Flush()
	return w.file.Close()
}
