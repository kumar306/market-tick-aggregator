package internal

import (
	"context"
	"sync"
)

var (
	resyncRegistry   = make(map[string]context.CancelFunc)
	resyncRegistryMu sync.RWMutex
)

// record the current connection attempt's cancel func for a topic, overwriting the previous one on every reconnect.
// each attempt gets a fresh context/cancel pair.
func RegisterResyncCancel(topic string, cancel context.CancelFunc) {
	resyncRegistryMu.Lock()
	defer resyncRegistryMu.Unlock()
	resyncRegistry[topic] = cancel
}

// forces the stream currently publishing to topic to tear down and reconnect.
func TriggerResync(topic string) bool {
	// use mu as normalizer can trigger resync at the same time another feed reconnects and accesses the same map to register new cancel
	resyncRegistryMu.RLock()
	cancel, ok := resyncRegistry[topic]
	resyncRegistryMu.RUnlock()
	if !ok {
		return false
	}
	cancel()
	return true
}
