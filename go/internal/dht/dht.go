package dht

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/JeroZp/p2p-lab/internal/common"
	"github.com/JeroZp/p2p-lab/internal/core"
)

// alpha is the concurrency parameter - how many parallel RPCs during lookup.
const alpha = 3

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
	Value []byte `json:"value"`
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
	store   map[string][]byte // local key-value storage
	storeMu sync.RWMutex

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
		store:   make(map[string][]byte),
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
		case msg := <-n.node.Inbound:
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
	value, found := n.store[payload.Key]
	n.storeMu.RUnlock()

	if found {
		replyPayload, _ := json.Marshal(ValueReplyPayload{Value: value})
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
	n.storeMu.Lock()
	n.store[payload.Key] = payload.Value
	n.storeMu.Unlock()
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

	if err := n.node.Send(to, msg); err != nil {
		return core.Message{}, err
	}

	select {
	case reply := <-ch:
		return reply, nil
	case <-time.After(timeout):
		return core.Message{}, fmt.Errorf("RPC timeout to %s", to)
	case <-n.ctx.Done():
		return core.Message{}, fmt.Errorf("DHT shutting down")
	}
}

// FindNode runs an iterative lookup for the K closest nodes to target.
func (n *Node) FindNode(target common.NodeID) []Entry {
	ctx, span := common.Tracer.Start(context.Background(), "dht.lookup")
	defer span.End()

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

		// Query them in parallel
		var mu sync.Mutex
		var wg sync.WaitGroup
		newNodes := make([]Entry, 0)

		for _, candidate := range toQuery {
			queried[candidate.ID] = true
			wg.Add(1)
			go func(c Entry) {
				defer wg.Done()

				// Child span per RPC hop
				_, rpcSpan := common.Tracer.Start(ctx, "dht.rpc")
				defer rpcSpan.End()
				rpcSpan.SetAttributes(
					attribute.String("dht.peer", c.ID.String()),
					attribute.String("dht.peer_addr", c.Addr),
				)

				payloadBytes, _ := json.Marshal(FindNodePayload{
					Target: target,
					Addr:   n.addr,
				})
				msg := core.Message{
					Type:    typeFindNode,
					From:    n.node.ID,
					ID:      fmt.Sprintf("fn-%s-%d", n.node.ID, time.Now().UnixNano()),
					Payload: payloadBytes,
				}

				reply, err := n.sendRPC(c.ID, msg, 5*time.Second)
				if err != nil {
					rpcSpan.SetStatus(codes.Error, err.Error())
					return
				}
				rpcSpan.SetStatus(codes.Ok, "")

				var replyPayload FindReplyPayload
				if err := json.Unmarshal(reply.Payload, &replyPayload); err != nil {
					return
				}

				mu.Lock()
				newNodes = append(newNodes, replyPayload.Nodes...)
				mu.Unlock()
			}(candidate)
		}
		wg.Wait()

		// Add new nodes to routing table and candidate list
		added := false
		for _, node := range newNodes {
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
	n.storeMu.Lock()
	n.store[key] = value
	n.storeMu.Unlock()

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
func (n *Node) FindValue(key string) ([]byte, bool) {
	// Check local store first
	n.storeMu.RLock()
	value, found := n.store[key]
	n.storeMu.RUnlock()
	if found {
		return value, true
	}

	keyID := common.NodeID(common.HashKey(key))
	candidates := n.table.Closest(keyID, K)
	queried := make(map[common.NodeID]bool)
	queried[n.node.ID] = true

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
				if err := json.Unmarshal(reply.Payload, &vr); err == nil {
					return vr.Value, true
				}
			}

			// Got a FindReply instead — add new nodes to candidates
			if reply.Type == typeFindReply {
				var fr FindReplyPayload
				if err := json.Unmarshal(reply.Payload, &fr); err == nil {
					for _, e := range fr.Nodes {
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
