package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/JeroZp/p2p-lab/internal/core"
	"github.com/prometheus/client_golang/prometheus"
)

// FanOut is the number of peers a node forwards a gossip message to.
// Higher = faster propagation, more network load.
// Lower = less load, slower convergence, higher risk of missing nodes.
const FanOut = 3

// defaultTTL is the maximum number of hops a message travels.
// A message with TTL=1 is only forwarded once, then dropped.
const defaultTTL = 5

// seenTTL is how long we remember a message ID to suppress duplicates.
const seenTTL = 10 * time.Minute

// GossipPayload is carried inside a core.Message's Payload field.
type GossipPayload struct {
	TTL  int    `json:"ttl"`
	Data []byte `json:"data"`
}

// seenEntry tracks when we swa a message so we can expire old entries.
type seenEntry struct {
	at time.Time
}

// Engine handles gossip for a node. It sits on top of core.Node,
// reading from its Inbound channel and forwarding gossip messages.
type Engine struct {
	node *core.Node
	seen map[string]seenEntry // message ID -> when we saw it
	mu   sync.Mutex           // protects seen map

	metrics *gossipMetrics

	Received chan []byte // delivers data to the application layer

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewEngine creates a gossip engine on top of an existing Node.
func NewEngine(node *core.Node) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		node:     node,
		seen:     make(map[string]seenEntry),
		metrics:  newGossipMetrics(),
		Received: make(chan []byte, 64),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// hasSeen returns true if we have already processed this mesage ID.
func (e *Engine) hasSeen(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.seen[id]
	return ok
}

// markSeen records a message ID in the seen cache.
func (e *Engine) markSeen(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seen[id] = seenEntry{at: time.Now()}
}

// evictSeen removes expired entries from the seen cache.
// Called periodically to prevent unbounded memory growth.
func (e *Engine) evictSeen() {
	e.mu.Lock()
	defer e.mu.Unlock()
	cutoff := time.Now().Add(-seenTTL)
	for id, entry := range e.seen {
		if entry.at.Before(cutoff) {
			delete(e.seen, id)
		}
	}
}

// Broadcast injects a new message into the network from this node.
// This is the entry point for the application to send gossip.
func (e *Engine) Broadcast(data []byte) error {
	id := fmt.Sprintf("%s-%d", e.node.ID, time.Now().UnixNano())

	payload := GossipPayload{
		TTL:  defaultTTL,
		Data: data,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshall gossip payload: %w", err)
	}

	msg := core.Message{
		Type:    core.TypeGossip,
		From:    e.node.ID,
		ID:      id,
		Payload: payloadBytes,
	}

	e.markSeen(id)
	return e.forward(msg)
}

// forward sends a message to FanOut randomly selected peers.
func (e *Engine) forward(msg core.Message) error {
	peers := e.node.Peers()
	if len(peers) == 0 {
		return nil
	}

	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	k := FanOut
	if k > len(peers) {
		k = len(peers)
	}

	for _, peerID := range peers[:k] {
		if err := e.node.Send(peerID, msg); err != nil {
			// peer might have just disconnected
			continue
		}
		e.metrics.forwardedTotal.Inc()
	}
	return nil
}

// handle processes an incomin gossip message.
func (e *Engine) handle(msg core.Message) {
	// decode the gossip payload
	var payload GossipPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return
	}

	// drop if we've seen this message before
	if e.hasSeen(msg.ID) {
		e.metrics.duplicatesTotal.Inc()
		return
	}
	e.markSeen(msg.ID)

	// deliver to the application layer
	select {
	case e.Received <- payload.Data:
	default:
		// drop if nobody is reading - don't block forwarding
	}

	// decrement TTL and forward if still alive
	if payload.TTL <= 1 {
		return
	}
	payload.TTL--

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return
	}
	msg.Payload = payloadBytes

	e.forward(msg)
}

// Start launches the gossip engine's background goroutines.
func (e *Engine) Start() {
	e.wg.Add(2)
	go e.processLoop()
	go e.evictLoop()
}

// processLoop reads from the node's Inbound channel and handles gossip messages.
func (e *Engine) processLoop() {
	defer e.wg.Done()
	for {
		select {
		case msg := <-e.node.Inbound:
			if msg.Type == core.TypeGossip {
				e.handle(msg)
			}
		case <-e.ctx.Done():
			return
		}
	}
}

// evictLoop periodically cleans the seen-message cache.
func (e *Engine) evictLoop() {
	defer e.wg.Done()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.evictLoop()
		case <-e.ctx.Done():
			return
		}
	}
}

// Stop shuts down the gossip engine cleanly.
func (e *Engine) Stop() {
	e.cancel()
	e.wg.Wait()
}

// In gossip.go — lets tests access the underlying node
func (e *Engine) Node() *core.Node {
	return e.node
}

// Registry returns the Prometheus registry for this engine.
func (e *Engine) Registry() *prometheus.Registry {
	return e.metrics.registry
}
