// Package ingestts carries the moment a market message was received from the exchange websocket along the pipeline,
// in a Kafka record header, so the last hop (persistence) can report how stale a row is the instant it becomes
// visible in Postgres: now minus the ingest time of the newest message that row reflects.
//
// A header was chosen over a protobuf field so no schema or generated code has to change, and a record that has
// no header (an older producer, a load generator) is simply skipped by the metric, not mis-measured.
package ingestts

import (
	"encoding/binary"

	"github.com/twmb/franz-go/pkg/kgo"
)

// HeaderKey is the Kafka record header holding the ingest time.
const HeaderKey = "ing"

// Encode stores unix milliseconds as 8 big-endian bytes.
func Encode(ms int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(ms))
	return b
}

// Header builds the header for ms.
func Header(ms int64) kgo.RecordHeader {
	return kgo.RecordHeader{Key: HeaderKey, Value: Encode(ms)}
}

// Find returns the ingest time in unix milliseconds, if the headers carry one.
func Find(headers []kgo.RecordHeader) (ms int64, ok bool) {
	for _, h := range headers {
		if h.Key == HeaderKey && len(h.Value) == 8 {
			return int64(binary.BigEndian.Uint64(h.Value)), true
		}
	}
	return 0, false
}
