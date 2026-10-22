package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/JeroZp/p2p-lab/internal/common"
	"github.com/JeroZp/p2p-lab/internal/core"
	"github.com/JeroZp/p2p-lab/internal/dht"
)

const (
	typeChunkRequest core.MessageType = "chunk_request"
	typeChunkReply   core.MessageType = "chunk_reply"

	maxParallel = 4						// chunks fetched at the same time
	chunkTimeout = 10 * time.Second		// per-chunk request timeout
)

// Manifest describes a shared file. It is stored in the DHT under the root hash.
type Manifest struct {
	Root	[32]byte	`json:"root"`
	Size	int64		`json:"size"`
	Chunks	[][32]byte	`json:"chunks"`
}

// Provider is stored in the DHT under each chunk hash: "who has this chunk?"
type Provider struct {
	ID		common.NodeID	`json:"id"`
	Addr	string			`json:"addr"`
}

type chunkRequest struct {
	Hash [32]byte `json:"hash"`
}

type chunkReply struct {
	Hash  [32]byte `json:"hash"`
	Data  []byte   `json:"data"`
	Found bool     `json:"found"`
}

// Engine shares and fetches files using the DHT as a directory
// and direct TCP messages as transport.
type Engine struct {
	node    *core.Node
	dht     *dht.Node
	addr    string
	inbound <-chan core.Message
	metrics *transferMetrics

	chunks   map[[32]byte][]byte // chunks this node can serve
	chunksMu sync.RWMutex

	pending   map[string]chan core.Message // in-flight requests by message ID
	pendingMu sync.Mutex
	seq       atomic.Uint64 // unique request IDs

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewEngine(node *core.Node, d *dht.Node, addr string) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		node:	 node,
		dht:	 d,
		addr:    addr,
		inbound: node.Subscribe(64, typeChunkRequest, typeChunkReply),
		metrics: newTransferMetrics(),
		chunks:  make(map[[32]byte][]byte),
		pending: make(map[string]chan core.Message),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func chunkKey(h [32]byte) string    { return "chunk:" + hex.EncodeToString(h[:]) }
func manifestKey(h [32]byte) string { return "manifest:" + hex.EncodeToString(h[:]) }

// Registry exposes transfer metrics for /metrics.
func (e *Engine) Registry() *prometheus.Registry {
	return e.metrics.registry
}

// Share chunks a file, keeps the chunks locally, and announces
// the manifest and every chunk in the DHT. Returns the manifest,
// whose Root is what other nodes need to fetch the file.
func (e *Engine) Share(path string) (Manifest, error) {
	chunks, err := ChunkFile(path, DefaultChunkSize)
	if err != nil {
		return Manifest{}, err
	}
	if len(chunks) == 0 {
		return Manifest{}, fmt.Errorf("cannot share empty file")
	}

	tree, err := NewMerkleTree(Hashes(chunks))
	if err != nil {
		return Manifest{}, err
	}

	var size int64
	e.chunksMu.Lock()
	for _, c := range chunks {
		e.chunks[c.Hash] = c.Data
		size += int64(len(c.Data))
	}
	e.chunksMu.Unlock()

	provider, _ := json.Marshal(Provider{ID: e.node.ID, Addr: e.addr})
	for _, c := range chunks {
		if err := e.dht.Store(chunkKey(c.Hash), provider); err != nil {
			return Manifest{}, fmt.Errorf("announce chunk %d: %w", c.Index, err)
		}
	}

	m := Manifest{Root: tree.Root(), Chunks: Hashes(chunks), Size: size}
	mBytes, _ := json.Marshal(m)
	if err := e.dht.Store(manifestKey(m.Root), mBytes); err != nil {
		return Manifest{}, fmt.Errorf("announce manifest: %w", err)
	}
	return m, nil
}

// FetchFile downloads a file by its Merkle root. It trusts nothing
// from peers: the manifest is checked against the root, and every
// chunk is checked against its hash.
func (e *Engine) FetchFile(root [32]byte) ([]byte, error) {
	ctx, span := common.Tracer.Start(context.Background(), "transfer.fetch_file")
	defer span.End()
	span.SetAttributes(attribute.String("transfer.root", hex.EncodeToString(root[:])))

	raw, ok := e.dht.FindValue(manifestKey(root))
	if !ok {
		span.SetStatus(codes.Error, "manifest not found")
		return nil, fmt.Errorf("manifest not found for root %x", root[:8])
	}

	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}

	// The manifest came from the network. Recompute its root to make
	// sure nobody handed us a fake chunk list.
	tree, err := NewMerkleTree(m.Chunks)
	if err != nil {
		return nil, err
	}
	if tree.Root() != root {
		span.SetStatus(codes.Error, "manifest root mismatch")
		return nil, fmt.Errorf("manifest does not match requested root")
	}

	results := make([]Chunk, len(m.Chunks))
	errs := make([]error, len(m.Chunks))
	sem := make(chan struct{}, maxParallel) // limits concurrent fetches
	var wg sync.WaitGroup

	for i, h := range m.Chunks {
		wg.Add(1)
		go func(i int, h [32]byte) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			_, cspan := common.Tracer.Start(ctx, "transfer.fetch_chunk")
			defer cspan.End()
			cspan.SetAttributes(attribute.Int("transfer.chunk_index", i))

			start := time.Now()
			data, err := e.fetchChunk(h)
			e.metrics.chunkDuration.Observe(time.Since(start).Seconds())

			if err != nil {
				errs[i] = err
				cspan.SetStatus(codes.Error, err.Error())
				return
			}
			results[i] = Chunk{Index: i, Hash: h, Data: data}
		}(i, h)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("chunk %d: %w", i, err)
		}
	}

	span.SetAttributes(attribute.Int("transfer.chunks", len(results)))
	return Reassemble(results)
}

// fetchChunk returns a chunk from local storage, or finds a provider
// in the DHT and requests the chunk from it.
func (e *Engine) fetchChunk(h [32]byte) ([]byte, error) {
	e.chunksMu.RLock()
	data, ok := e.chunks[h]
	e.chunksMu.RUnlock()
	if ok {
		return data, nil
	}

	raw, ok := e.dht.FindValue(chunkKey(h))
	if !ok {
		return nil, fmt.Errorf("no provider for chunk %x", h[:8])
	}
	var p Provider
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode provider: %w", err)
	}

	payload, _ := json.Marshal(chunkRequest{Hash: h})
	msg := core.Message{
		Type:    typeChunkRequest,
		From:    e.node.ID,
		ID:      fmt.Sprintf("cr-%d", e.seq.Add(1)),
		Payload: payload,
	}

	reply, err := e.request(p.ID, msg)
	if err != nil {
		return nil, err
	}

	var cr chunkReply
	if err := json.Unmarshal(reply.Payload, &cr); err != nil {
		return nil, fmt.Errorf("decode chunk reply: %w", err)
	}
	if !cr.Found {
		return nil, fmt.Errorf("provider %s does not have chunk", p.ID)
	}

	// Never trust a peer: verify before accepting.
	if sha256.Sum256(cr.Data) != h {
		e.metrics.verifyFailures.Inc()
		return nil, fmt.Errorf("peer %s sent corrupted chunk", p.ID)
	}

	e.metrics.bytesTotal.Add(float64(len(cr.Data)))

	// Keep it, so this node can serve the chunk to others too.
	e.chunksMu.Lock()
	e.chunks[h] = cr.Data
	e.chunksMu.Unlock()

	return cr.Data, nil
}

// request sends a message and waits for the reply with the same ID.
func (e *Engine) request(to common.NodeID, msg core.Message) (core.Message, error) {
	ch := make(chan core.Message, 1)
	e.pendingMu.Lock()
	e.pending[msg.ID] = ch
	e.pendingMu.Unlock()
	defer func() {
		e.pendingMu.Lock()
		delete(e.pending, msg.ID)
		e.pendingMu.Unlock()
	}()

	if err := e.node.Send(to, msg); err != nil {
		return core.Message{}, err
	}

	select {
	case reply := <-ch:
		return reply, nil
	case <-time.After(chunkTimeout):
		return core.Message{}, fmt.Errorf("chunk request to %s timed out", to)
	case <-e.ctx.Done():
		return core.Message{}, fmt.Errorf("transfer engine stopped")
	}
}

// Start launches the message loop.
func (e *Engine) Start() {
	e.wg.Add(1)
	go e.processLoop()
}

func (e *Engine) processLoop() {
	defer e.wg.Done()
	for {
		select {
		case msg := <-e.inbound:
			switch msg.Type {
			case typeChunkRequest:
				e.handleRequest(msg)
			case typeChunkReply:
				e.handleReply(msg)
			}
		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Engine) handleRequest(msg core.Message) {
	var req chunkRequest
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		return
	}

	e.chunksMu.RLock()
	data, ok := e.chunks[req.Hash]
	e.chunksMu.RUnlock()

	payload, _ := json.Marshal(chunkReply{Hash: req.Hash, Data: data, Found: ok})
	e.node.Send(msg.From, core.Message{
		Type:    typeChunkReply,
		From:    e.node.ID,
		ID:      msg.ID, // same ID so the requester can match it
		Payload: payload,
	})
}

func (e *Engine) handleReply(msg core.Message) {
	e.pendingMu.Lock()
	ch, ok := e.pending[msg.ID]
	e.pendingMu.Unlock()
	if ok {
		select {
		case ch <- msg:
		default:
		}
	}
}

// Stop shuts down the engine.
func (e *Engine) Stop() {
	e.cancel()
	e.wg.Wait()
}