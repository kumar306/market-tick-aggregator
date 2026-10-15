package kafkatest

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DelayDialer returns a kgo option that adds rtt of round-trip latency to every broker connection
// (rtt/2 each way), so a local container behaves like a broker across a slow link.
// Order and pipelining are preserved: a delayed byte stream, not a per-call sleep.
// Deadlines are ignored on delayed connections, since the delay queue sits in front of the real socket.
func DelayDialer(rtt time.Duration) kgo.Opt {
	return kgo.Dialer(func(ctx context.Context, network, host string) (net.Conn, error) {
		c, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, host)
		if err != nil || rtt <= 0 {
			return c, err
		}
		return newDelayConn(c, rtt/2), nil
	})
}

type chunk struct {
	b   []byte
	at  time.Time
	err error
}

type delayConn struct {
	net.Conn
	d         time.Duration
	in, out   chan chunk
	left      []byte
	done      chan struct{}
	closeOnce sync.Once
}

func newDelayConn(c net.Conn, d time.Duration) *delayConn {
	dc := &delayConn{Conn: c, d: d, in: make(chan chunk, 8192), out: make(chan chunk, 8192), done: make(chan struct{})}

	// pump bytes off the real socket, stamped with arrival time
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := c.Read(buf)
			if n > 0 {
				dc.in <- chunk{b: append([]byte(nil), buf[:n]...), at: time.Now()}
			}
			if err != nil {
				dc.in <- chunk{err: err, at: time.Now()}
				return
			}
		}
	}()

	// release writes onto the real socket d after they were issued
	go func() {
		for {
			select {
			case ch := <-dc.out:
				if w := time.Until(ch.at.Add(d)); w > 0 {
					time.Sleep(w)
				}
				if _, err := c.Write(ch.b); err != nil {
					return
				}
			case <-dc.done:
				return
			}
		}
	}()
	return dc
}

func (c *delayConn) Read(p []byte) (int, error) {
	if len(c.left) == 0 {
		select {
		case ch := <-c.in:
			if w := time.Until(ch.at.Add(c.d)); w > 0 {
				time.Sleep(w)
			}
			if ch.err != nil {
				return 0, ch.err
			}
			c.left = ch.b
		case <-c.done:
			return 0, net.ErrClosed
		}
	}
	n := copy(p, c.left)
	c.left = c.left[n:]
	return n, nil
}

func (c *delayConn) Write(p []byte) (int, error) {
	select {
	case c.out <- chunk{b: append([]byte(nil), p...), at: time.Now()}:
		return len(p), nil
	case <-c.done:
		return 0, net.ErrClosed
	}
}

func (c *delayConn) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	return c.Conn.Close()
}

func (c *delayConn) SetDeadline(time.Time) error      { return nil }
func (c *delayConn) SetReadDeadline(time.Time) error  { return nil }
func (c *delayConn) SetWriteDeadline(time.Time) error { return nil }
