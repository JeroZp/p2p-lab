package core

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"math/rand"
	"time"

	"github.com/JeroZp/p2p-lab/internal/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	reconcileInterval = 1 * time.Second
	backoffBase       = 500 * time.Millisecond
	backoffMax        = 5 * time.Second
)

// Node is a P2P node. It listens for incoming connections,
// dials outgoing connections, and manages a pool of active peers.
type Node struct {
	ID      common.NodeID
	keyPair common.KeyPair

	listener net.Listener
	peers    map[common.NodeID]*Conn
	mu       sync.RWMutex

	subs   map[MessageType]chan Message // type → subscriber channel
	subsMu sync.RWMutex

	Inbound chan Message // all received messages land here

	metrics *coreMetrics

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewNode creates a Node, generating and loading its identity from keyPath.
func NewNode(keyPath string) (*Node, error) {
	kp, id, err := common.LoadOrCreate(keyPath)
	if err != nil {
		return nil, fmt.Errorf("load identity: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Node{
		ID:      id,
		keyPair: kp,
		peers:   make(map[common.NodeID]*Conn),
		subs:    make(map[MessageType]chan Message),
		Inbound: make(chan Message, 128),
		metrics: newCoreMetrics(),
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// Listen starts accepting incoming TCP connections on the given address.
// It runs on the background until the node shuts down.
func (n *Node) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	n.listener = ln

	n.wg.Add(1)
	go n.acceptLoop()

	return nil
}

// acceptLoop runs in the background, accepting new connections.
func (n *Node) acceptLoop() {
	defer n.wg.Done()

	for {
		tc, error := n.listener.Accept()
		if error != nil {
			select {
			case <-n.ctx.Done():
				return // shutdown requested
			default:
				// listener error - log and continue
				continue
			}
		}

		// We don't know the peer's NodeID yet — they will send
		// a handshake message. For now we use a zero NodeID
		// as a placeholder until the handshake is complete.
		var placeholder common.NodeID
		conn := newConn(n.ctx, placeholder, tc)
		conn.start()

		n.wg.Add(1)
		go n.serveConn(conn)
	}
}

// Dial connects to a peer once.
func (n *Node) Dial(addr string) error {
	_, err := n.dialConn(addr)
	return err
}

// dialConn connects to addr, sends our handshake, and returns the connection.
func (n *Node) dialConn(addr string) (*Conn, error) {
	tc, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	var placeholder common.NodeID
	conn := newConn(n.ctx, placeholder, tc)
	conn.start()

	handshake := Message{Type: TypeHandshake, From: n.ID, ID: "handshake"}
	if err := conn.Send(handshake); err != nil {
		conn.close()
		return nil, fmt.Errorf("send handshake: %w", err)
	}

	n.wg.Add(1)
	go n.serveConn(conn)
	return conn, nil
}

// serveConn reads messsages from a connection and routes them.
// it handles the handshake and then forwards all messages to inbound.
func (n *Node) serveConn(conn *Conn) {
	defer n.wg.Done()
	defer n.removePeer(conn)
	defer conn.close()

	handshakeDone := false

	for {
		select {
		case msg := <-conn.inbound:
			if msg.Type == TypeHandshake {
				if handshakeDone {	// ignore duplicate handshakes
					continue
				}
				// now we know the peer's NodeID - register them properly
				conn.id = msg.From
				n.addPeer(conn)
				log.Printf("peer connected: %s", msg.From)
				handshakeDone = true

				if conn.id != n.ID {
					reply := Message{Type: TypeHandshake, From: n.ID, ID: "handshake"}
					conn.Send(reply)
				}
				continue
			}

			n.metrics.messagesReceivedTotal.WithLabelValues(string(msg.Type)).Inc()

			// forward all other message to the node's inbound channel
			if !n.dispatch(msg) {
				return
			}
		case <-conn.ctx.Done():
			return
		case <-n.ctx.Done():
			return
		}
	}
}

// addPeer registers a connection in the peer pool.
func (n *Node) addPeer(conn *Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if old, ok := n.peers[conn.id]; ok && old != conn {
		old.close() // its cleanup is a no-op: removePeer checks it's still registered
	} else {
		n.metrics.connectionsActive.Inc()
	}
	n.peers[conn.id] = conn
}

// removePeer removes a connection from the pool, but only if it is
// still the registered connection for that peer. Prevents a stale
// connection's cleanup from removing a newer connection to the same peer.
func (n *Node) removePeer(conn *Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if current, ok := n.peers[conn.id]; ok && current == conn {
		delete(n.peers, conn.id)
		n.metrics.connectionsActive.Dec()
	}
}

func (n *Node) Send(to common.NodeID, msg Message) error {
	n.mu.RLock()
	conn, ok := n.peers[to]
	n.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no connection to peer %s", to)
	}

	n.metrics.messagesSentTotal.WithLabelValues(string(msg.Type)).Inc()
	return conn.Send(msg)
}

// Peers returns the NodeIDs of all currently connected peers.
func (n *Node) Peers() []common.NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	ids := make([]common.NodeID, 0, len(n.peers))
	for id := range n.peers {
		ids = append(ids, id)
	}
	return ids
}

// Shutdown gracefully stops the node — closes the listener,
// cancels all connections, and waits for goroutines to exit.
func (n *Node) Shutdown() {
	n.cancel()
	if n.listener != nil {
		n.listener.Close()
	}

	n.mu.RLock()
	for _, conn := range n.peers {
		conn.close()
	}
	n.mu.RUnlock()

	n.wg.Wait()
}

// Addr returns the address the node is listening on.
func (n *Node) Addr() string {
	if n.listener == nil {
		return ""
	}
	return n.listener.Addr().String()
}

// Registry returns the Prometheus registry for this node.
func (n *Node) Registry() *prometheus.Registry {
	return n.metrics.registry
}

// Subscribe returns a channel that receives every message whose type
// is in types. Messages with no subscriber fall through to Inbound.
// Call it before connecting to peers so no messages are missed.
func (n *Node) Subscribe(buffer int, types ...MessageType) <-chan Message {
	ch := make(chan Message, buffer)
	n.subsMu.Lock()
	defer n.subsMu.Unlock()
	for _, t := range types {
		n.subs[t] = ch
	}
	return ch
}

// dispatch delivers a message to its subscriber, or to Inbound if none.
// Returns false if the node is shutting down.
func (n *Node) dispatch(msg Message) bool {
	n.subsMu.RLock()
	ch, ok := n.subs[msg.Type]
	n.subsMu.RUnlock()
	if !ok {
		ch = n.Inbound
	}
	select {
	case ch <- msg:
		return true
	case <-n.ctx.Done():
		return false
	}
}

// Maintain keeps this node connected to every address in addrs.
// If a connection dies, it is redialed with exponential backoff.
// Runs in the background until the node shuts down.
func (n *Node) Maintain(addrs []string) {
	n.wg.Add(1)
	go n.maintainLoop(addrs)
}

func (n *Node) maintainLoop(addrs []string) {
	defer n.wg.Done()

	type target struct {
		conn    *Conn         // the connection we dialed, if any
		backoff time.Duration // current backoff (without jitter)
		nextTry time.Time     // don't dial before this
	}
	targets := make(map[string]*target, len(addrs))
	for _, a := range addrs {
		targets[a] = &target{}
	}

	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		// Reconcile: desired = every address, actual = live connections
		for addr, t := range targets {
			if t.conn != nil && t.conn.State() == StateConnected {
				t.backoff = 0
				continue
			}
			if time.Now().Before(t.nextTry) {
				continue
			}

			conn, err := n.dialConn(addr)
			if err != nil {
				t.backoff = nextBackoff(t.backoff)
				wait := withJitter(t.backoff)
				t.nextTry = time.Now().Add(wait)
				log.Printf("reconnect to %s failed: %v (retry in %v)", addr, err, wait.Round(100*time.Millisecond))
				continue
			}
			t.conn = conn
			log.Printf("connected to %s", addr)
		}

		select {
		case <-ticker.C:
		case <-n.ctx.Done():
			return
		}
	}
}

// nextBackoff doubles the backoff, starting at backoffBase, capped at backoffMax.
func nextBackoff(cur time.Duration) time.Duration {
	if cur == 0 {
		return backoffBase
	}
	if cur*2 > backoffMax {
		return backoffMax
	}
	return cur * 2
}

// withJitter adds ±20% random variation so peers don't retry in lockstep.
func withJitter(d time.Duration) time.Duration {
	j := int64(d) / 5
	return d + time.Duration(rand.Int63n(2*j+1)-j)
}