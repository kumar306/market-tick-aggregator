package internal

import "sync"

// typical exchange tick/depth JSON payload size, so the common case needs
// zero growth inside bytes.Buffer.ReadFrom.
const expectedMessageSize = 1024

// shared across all 6 exchange/channel streams as sync.Pool shards safely across concurrent goroutines
// pools *[]byte, not []byte as get/put take interface{}, and boxing a []byte into an interface
// allocates a copy on the heap for the interface to point at
var messageBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, expectedMessageSize)
		return &buf
	},
}
