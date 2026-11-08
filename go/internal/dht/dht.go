package dht

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/JeroZp/p2p-lab/internal/common"
	"github.com/JeroZp/p2p-lab/internal/core"
)

// alpha is the concurrency parameter - how many parallel RPCs during lookup.
const alpha = 3

// maxValuesPerKey caps the set, so a popular key can't grow forever.
const maxValuesPerKey = 20

// stragglerWait is how long a lookup round waits for the remaining
// replies once the first one has arrived. Slow peers don't hold up
// the whole lookup.
const stragglerWait = 50 * time.Millisecond

// DHT message types
const (
	typeFindNode   core.MessageType = "dht_find_node"
	typeFindReply  core.MessageType = "dht_find_reply"
	typeStore      core.MessageType = "dht_store"
	typeFindValue  core.MessageType = "dht_find_value"
	typeValueReply core.MessageType = "dht_value_reply"
)

// FindNodePayload is the payload for a FindNope RPC request.
type FindNodePayload struct {
	Target common.NodeID `json:"target"`
	Addr   string        `json:"addr"` // sender's listening address
}

// FindReplyPayload is the response to a FindNode or FindValue RPC.
type FindReplyPayload struct {
	Nodes []Entry `json:"nodes"`
}

// StorePayload carries a key-value pair to store.
type StorePayload struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// ValueReplyPayload carries a found value back to the requester.
type ValueReplyPayload struct {
	Values [][]byte `json:"values"`
}

// pendingReply tracks an in-flight RPC waiting for a response.
type pendingReply struct {
	ch chan core.Message
}

// Node is a DHT participant. It wraps core.Node and Adds
// routing table management and key-value storage.
type Node struct {
	node    *core.Node
	addr    string // this node's listening address
	table   *RoutingTable
	store   map[string][][]byte // key -> set of values
	storeMu sync.RWMutex

	inbound <-chan core.Message

	metrics *dhtMetrics

	pending   map[string]*pendingReply // in-flight RPCs keyed by message ID
	pendingMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDHTNode creates a DHT node on top of an existing core.Node.
func NewDHTNode(node *core.Node, addr string) *Node {
	ctx, cancel := context.WithCancel(context.Background())
	return &Node{
		node:    node,
		addr:    addr,
		table:   NewRoutingTable(node.ID),
		store:   make(map[string][][]byte),
		inbound: node.Subscribe(128, typeFindNode, typeFindReply, typeStore, typeFindValue, typeValueReply),
		metrics: newDHTMetrics(),
		pending: make(map[string]*pendingReply),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start launches the DHT's message processing loop.
func (n *Node) Start() {
	n.wg.Add(1)
	go n.processLoop()
}

// processLoop reads DHT messages from the node's Inbound channel.
func (n *Node) processLoop() {
	defer n.wg.Done()
	for {
		select {
		case msg := <-n.inbound:
			n.handle(msg)
		case <-n.ctx.Done():
			return
		}
	}
}

// handle dispatches incoming DHT messages to the right handler.
func (n *Node) handle(msg core.Message) {
	switch msg.Type {
	case typeFindNode:
		n.handleFindNode(msg)
	case typeFindValue:
		n.handleFindValue(msg)
	case typeStore:
		n.handleStore(msg)
	case typeFindReply, typeValueReply:
		n.handleReply(msg)
	}
}

// handleFindNode responds with the K closest nodes we know to the target.
func (n *Node) handleFindNode(msg core.Message) {
	var payload FindNodePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return
	}

	// Learn about the sender
	n.table.Add(Entry{ID: msg.From, Addr: payload.Addr})

	closest := n.table.Closest(payload.Target, K)
	replyPayload, _ := json.Marshal(FindReplyPayload{Nodes: closest})

	reply := core.Message{
		Type:    typeFindReply,
		From:    n.node.ID,
		ID:      msg.ID, // same ID so sender can match reply to request
		Payload: replyPayload,
	}
	n.node.Send(msg.From, reply)
}

// handleFindValue checks local store first, falls back to closest nodes.
func (n *Node) handleFindValue(msg core.Message) {
	var payload StorePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return
	}

	n.storeMu.RLock()
	values, found := n.store[payload.Key]
	n.storeMu.RUnlock()

	if found {
		replyPayload, _ := json.Marshal(ValueReplyPayload{Values: values}) // return the first value found
		reply := core.Message{
			Type:    typeValueReply,
			From:    n.node.ID,
			ID:      msg.ID,
			Payload: replyPayload,
		}
		n.node.Send(msg.From, reply)
		return
	}

	// Don't have it — reply with closest nodes instead
	n.handleFindNode(msg)
}

// handleStore saves a key-value pair locally.
func (n *Node) handleStore(msg core.Message) {
	var payload StorePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return
	}
	n.addValue(payload.Key, payload.Value)
}

// handleReply delivers an RPC reply to the waiting goroutine.
func (n *Node) handleReply(msg core.Message) {
	n.pendingMu.Lock()
	p, ok := n.pending[msg.ID]
	n.pendingMu.Unlock()

	if ok {
		select {
		case p.ch <- msg:
		default:
		}
	}
}

// sendRPC sends a message and waits for a reply with a timeout.
func (n *Node) sendRPC(to common.NodeID, msg core.Message, timeout time.Duration) (core.Message, error) {
	ch := make(chan core.Message, 1)

	n.pendingMu.Lock()
	n.pending[msg.ID] = &pendingReply{ch: ch}
	n.pendingMu.Unlock()

	defer func() {
		n.pendingMu.Lock()
		delete(n.pending, msg.ID)
		n.pendingMu.Unlock()
	}()

	peerDone := n.node.PeerDone(to)

	if err := n.node.Send(to, msg); err != nil {
		return core.Message{}, err
	}

	select {
	case reply := <-ch:
		return reply, nil
	case <-peerDone:
		return core.Message{}, fmt.Errorf("peer %s disconnected", to)
	case <-time.After(timeout):
		return core.Message{}, fmt.Errorf("RPC timeout to %s", to)
	case <-n.ctx.Done():
		return core.Message{}, fmt.Errorf("DHT shutting down")
	}
}

// FindNode runs an iterative lookup for the K closest nodes to target.
func (n *Node) FindNode(target common.NodeID) []Entry {
	start := time.Now()
	ctx, span := common.Tracer.Start(context.Background(), "dht.lookup")
	defer span.End()
	defer func() {
		n.metrics.lookupDuration.Observe(time.Since(start).Seconds())
		n.metrics.routingTableSize.Set(float64(n.table.Size()))
	}()

	span.SetAttributes(
		attribute.String("dht.target", target.String()),
		attribute.String("dht.self", n.node.ID.String()),
	)

	// Seed with our own closest known nodes
	candidates := n.table.Closest(target, K)
	if len(candidates) == 0 {
		return nil
	}

	queried := make(map[common.NodeID]bool)
	queried[n.node.ID] = true

	// seen tracks every node already in (or rejected from) candidates,
	// so the same node is never added twice, and we never add ourselves.
	seen := make(map[common.NodeID]bool)
	seen[n.node.ID] = true
	for _, c := range candidates {
		seen[c.ID] = true
	}

	for {
		// Pick alpha unqueried candidates closest to target
		var toQuery []Entry
		for _, c := range candidates {
			if !queried[c.ID] {
				toQuery = append(toQuery, c)
				if len(toQuery) == alpha {
					break
				}
			}
		}
		if len(toQuery) == 0 {
			break
		}

		// Buffered for every RPC: late goroutines can always send their
		// result and exit, even after this round has moved on.
		replies := make(chan []Entry, len(toQuery))

		for _, candidate := range toQuery {
			queried[candidate.ID] = true
			go func(c Entry) {
				_, rpcSpan := common.Tracer.Start(ctx, "dht.rpc")
				defer rpcSpan.End()
				rpcSpan.SetAttributes(
					attribute.String("dht.peer", c.ID.String()),
					attribute.String("dht.peer_addr", c.Addr),
				)

				payloadBytes, _ := json.Marshal(FindNodePayload{Target: target, Addr: n.addr})
				msg := core.Message{
					Type:    typeFindNode,
					From:    n.node.ID,
					ID:      fmt.Sprintf("fn-%s-%d", n.node.ID, time.Now().UnixNano()),
					Payload: payloadBytes,
				}

				reply, err := n.sendRPC(c.ID, msg, 5*time.Second)
				if err != nil {
					rpcSpan.SetStatus(codes.Error, err.Error())
					replies <- nil
					return
				}

				var rp FindReplyPayload
				if err := json.Unmarshal(reply.Payload, &rp); err != nil {
					replies <- nil
					return
				}
				rpcSpan.SetStatus(codes.Ok, "")
				replies <- rp.Nodes
			}(candidate)
		}

		// Collect replies until all are in, or until stragglerWait
		// has passed since the first successful reply.
		var newNodes []Entry
		var straggler <-chan time.Time // nil = timer not started yet
	collect:
		for received := 0; received < len(toQuery); {
			select {
			case nodes := <-replies:
				received++
				newNodes = append(newNodes, nodes...)
				if nodes != nil && straggler == nil {
					straggler = time.After(stragglerWait)
				}
			case <-straggler:
				break collect // don't wait for slow peers
			}
		}

		// Add new nodes to routing table and candidate list
		added := false
		for _, node := range newNodes {
			if seen[node.ID] {
				continue
			}
			seen[node.ID] = true

			n.table.Add(node)
			// Check if this node is closer than our current worst candidate
			if len(candidates) < K {
				candidates = append(candidates, node)
				added = true
			} else {
				worstDist := target.XORDistance(candidates[len(candidates)-1].ID)
				newDist := target.XORDistance(node.ID)
				for i := 0; i < 32; i++ {
					if newDist[i] < worstDist[i] {
						candidates[len(candidates)-1] = node
						added = true
						break
					} else if newDist[i] > worstDist[i] {
						break
					}
				}
			}
		}

		// Re-sort candidates by distance to target
		sortByDistance(candidates, target)

		if !added {
			break // no progress — we've converged
		}
	}

	span.SetAttributes(attribute.Int("dht.results", len(candidates)))
	span.SetStatus(codes.Ok, "")

	if len(candidates) > K {
		return candidates[:K]
	}
	return candidates
}

// sortByDistance sorts entries by XOR distance to target ascending.
func sortByDistance(entries []Entry, target common.NodeID) {
	sort.Slice(entries, func(i, j int) bool {
		di := target.XORDistance(entries[i].ID)
		dj := target.XORDistance(entries[j].ID)
		for k := 0; k < 32; k++ {
			if di[k] != dj[k] {
				return di[k] < dj[k]
			}
		}
		return false
	})
}

// Store saves a key-value pair at the K closest nodes to the key.
func (n *Node) Store(key string, value []byte) error {
	// Always store locally as well as at closest nodes
	n.addValue(key, value)

	keyID := common.NodeID(common.HashKey(key))
	closest := n.FindNode(keyID)

	for _, entry := range closest {
		payload, _ := json.Marshal(StorePayload{Key: key, Value: value})
		msg := core.Message{
			Type:    typeStore,
			From:    n.node.ID,
			ID:      fmt.Sprintf("store-%s-%d", key, time.Now().UnixNano()),
			Payload: payload,
		}
		// Non-fatal — best effort replication
		n.node.Send(entry.ID, msg)
	}
	return nil
}

// FindValue retrieves a value by key from the DHT.
func (n *Node) FindValue(key string) ([][]byte, bool) {
	if values, ok := n.localValues(key); ok {
		return values, true
	}
	// Check local store first
	n.storeMu.RLock()
	values, found := n.store[key]
	n.storeMu.RUnlock()
	if found {
		return values, true
	}

	keyID := common.NodeID(common.HashKey(key))
	candidates := n.table.Closest(keyID, K)
	queried := make(map[common.NodeID]bool)
	queried[n.node.ID] = true

	// the same as FindNode, but we also check for a value reply and return it if found
	seen := make(map[common.NodeID]bool)
	seen[n.node.ID] = true
	for _, c := range candidates {
		seen[c.ID] = true
	}

	for {
		var toQuery []Entry
		for _, c := range candidates {
			if !queried[c.ID] {
				toQuery = append(toQuery, c)
				if len(toQuery) == alpha {
					break
				}
			}
		}
		if len(toQuery) == 0 {
			break
		}

		for _, candidate := range toQuery {
			queried[candidate.ID] = true

			payloadBytes, _ := json.Marshal(StorePayload{Key: key})
			msg := core.Message{
				Type:    typeFindValue,
				From:    n.node.ID,
				ID:      fmt.Sprintf("fv-%s-%d", key, time.Now().UnixNano()),
				Payload: payloadBytes,
			}

			reply, err := n.sendRPC(candidate.ID, msg, 5*time.Second)
			if err != nil {
				continue
			}

			if reply.Type == typeValueReply {
				var vr ValueReplyPayload
				if err := json.Unmarshal(reply.Payload, &vr); err == nil && len(vr.Values) > 0 {
					return vr.Values, true
				}
			}

			// Got a FindReply instead — add new nodes to candidates
			if reply.Type == typeFindReply {
				var fr FindReplyPayload
				if err := json.Unmarshal(reply.Payload, &fr); err == nil {
					for _, e := range fr.Nodes {
						if seen[e.ID] {
							continue
						}
						seen[e.ID] = true
						n.table.Add(e)
						candidates = append(candidates, e)
					}
					sortByDistance(candidates, keyID)
				}
			}
		}
	}

	return nil, false
}

// Stop shuts down the DHT node cleanly.
func (n *Node) Stop() {
	n.cancel()
	n.wg.Wait()
}

func (n *Node) Registry() *prometheus.Registry {
	return n.metrics.registry
}

func (n *Node) AddNode(e Entry) {
	n.table.Add(e)
}

// addValue adds value to the set stored under key, ignoring duplicates.
func (n *Node) addValue(key string, value []byte) {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()
	for _, v := range n.store[key] {
		if bytes.Equal(v, value) {
			return
		}
	}
	if len(n.store[key]) >= maxValuesPerKey {
		n.store[key] = n.store[key][1:] // drop the oldest
	}
	n.store[key] = append(n.store[key], value)
}

// localValues returns a copy of the values stored locally under key.
func (n *Node) localValues(key string) ([][]byte, bool) {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()
	vals, ok := n.store[key]
	if !ok {
		return nil, false
	}
	out := make([][]byte, len(vals))
	copy(out, vals)
	return out, true
}
