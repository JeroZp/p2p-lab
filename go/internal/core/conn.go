package core

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/JeroZp/p2p-lab/internal/common"
)

type ConnState int

const (
	StateConnection ConnState = iota
	StateConnected
	StateClosing
	StateDead
)

type Conn struct {
	id    common.NodeID // Identify of the remote peer
	conn  net.Conn      // underlying TCP connection
	state ConnState
	mu    sync.RWMutex // protects state

	outbound chan Message // messages to send go here
	inbound  chan Message // messages receive land here

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // tracks read/write goroutines
}

// newConn wraps an stablished net.Conn into a managed Conn.
func newConn(ctx context.Context, peerID common.NodeID, c net.Conn) *Conn {
	ctx, cancel := context.WithCancel(ctx)
	return &Conn{
		id:       peerID,
		conn:     c,
		state:    StateConnected,
		outbound: make(chan Message, 32),
		inbound:  make(chan Message, 32),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// State returns the current connection state.
func (c *Conn) State() ConnState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// SetState updates the connection state safely.
func (c *Conn) SetState(s ConnState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = s
}

func (c *Conn) Send(msg Message) error {
	if c.State() != StateConnected {
		return fmt.Errorf("connection to %s is not active", c.id)
	}
	select {
	case c.outbound <- msg:
		return nil
	case <-c.ctx.Done():
		return fmt.Errorf("connection closing")
	}
}

// readLoop continuously reads messages from the TCP connection
// and puts them on the inbound channel.
func (c *Conn) readLoop() {
	defer c.wg.Done()
	defer c.close()

	for {
		msg, err := ReadMessage(c.conn)
		if err != nil {
			select {
			case <-c.ctx.Done():
				// shutdown was requested, not an error
			default:
				// unexpected error - peer probably died
			}
			return
		}

		select {
		case c.inbound <- msg:
		case <-c.ctx.Done():
			return
		}
	}
}

// writeLoop continuously reads from the outbound channel
// and writes messages to the TCP connection.
func (c *Conn) writeLoop() {
	defer c.wg.Done()

	for {
		select {
		case msg := <-c.outbound:
			if err := WriteMessage(c.conn, msg); err != nil {
				c.close()
				return
			}
		case <- c.ctx.Done():
			return
		}
	}
}

// start launches the read and write goroutines.
func (c *Conn) start() {
	c.wg.Add(2)
	go c.readLoop()
	go c.writeLoop()
}

// close transitions the connection to dead and clean up resources.
// Safe to call multiple times.
func (c *Conn) close() {
	c.SetState(StateClosing)
	c.cancel()
	c.conn.Close()
	c.SetState(StateDead)
}

// Wait blocks until both goroutines have exited.
func (c *Conn) Wait() {
	c.wg.Wait()
}
