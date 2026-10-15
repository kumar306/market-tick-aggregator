// Package srcoffset encodes where an output record came from (source topic, partition, offset) so a
// downstream consumer can drop a replayed copy without any external store.
//
// Without Kafka transactions a crashed producer re-sends everything since its last offset commit.
// Those copies carry the same source coordinate as the originals, so a consumer that remembers which
// coordinates it already applied can drop them from memory.
package srcoffset

import "encoding/binary"

// HeaderKey is the Kafka record header that carries the encoded source coordinate.
const HeaderKey = "src"

// Encode lays the coordinate out as 8 bytes offset | 4 bytes partition | topic bytes.
func Encode(topic string, partition int32, offset int64) []byte {
	b := make([]byte, 12+len(topic))
	binary.BigEndian.PutUint64(b[0:8], uint64(offset))
	binary.BigEndian.PutUint32(b[8:12], uint32(partition))
	copy(b[12:], topic)
	return b
}

// Decode returns the topic as a sub-slice (no allocation) so callers can use it directly as a map key
// via string(topic) in a lookup expression.
func Decode(v []byte) (topic []byte, partition int32, offset int64, ok bool) {
	if len(v) < 12 {
		return nil, 0, 0, false
	}
	offset = int64(binary.BigEndian.Uint64(v[0:8]))
	partition = int32(binary.BigEndian.Uint32(v[8:12]))
	return v[12:], partition, offset, true
}
