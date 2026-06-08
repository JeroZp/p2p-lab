package core

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/JeroZp/p2p-lab/internal/common"
	"github.com/prometheus/client_golang/prometheus"
)

// Node is a P2P node. It listens for incoming connections,
// dials outgoing connections, and manages a pool of active peers.
type Node struct {
	ID      common.NodeID
	keyPair common.KeyPair

	listener net.Listener
	peers    map[common.NodeID]*Conn
	mu       sync.RWMutex

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

// Dial connects to a remote node at the given address and performs a handshake to exchange NodeIDs.
func (n *Node) Dial(addr string) error {
	tc, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}

	var placeholder common.NodeID
	conn := newConn(n.ctx, placeholder, tc)
	conn.start()

	// Send our handshake immediatly so the remote node knows who we are
	handshake := Message{
		Type: TypeHandshake,
		From: n.ID,
		ID:   "handshake",
	}

	if err := conn.Send(handshake); err != nil {
		conn.close()
		return fmt.Errorf("send hanshake: %w", err)
	}

	n.wg.Add(1)
	go n.serveConn(conn)

	return nil
}

// serveConn reads messsages from a connection and routes them.
// it handles the handshake and then forwards all messages to inbound.
func (n *Node) serveConn(conn *Conn) {
	defer n.wg.Done()
	defer n.removePeer(conn.id)

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
			select {
			case n.Inbound <- msg:
			case <-n.ctx.Done():
				return
			}
		case <-n.ctx.Done():
			return
		}
	}
}

// addPeer registers a connection in the peer pool.
func (n *Node) addPeer(conn *Conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[conn.id] = conn
	n.metrics.connectionsActive.Inc()
}

// removePeer removes a connection from the peer pool.
func (n *Node) removePeer(id common.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.peers, id)
	n.metrics.connectionsActive.Dec()
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
