package dht

import (
	"sort"
	"sync"

	"github.com/JeroZp/p2p-lab/internal/common"
)
// Maximum number of entries per k-buckets for our small cluster.
const K = 8

// Entry represents a known node in the routing table
type Entry struct {
	ID		common.NodeID
	Addr 	string	// "host:port" - needed to dial this node
}

// bucket holds up to K entries for a specific region of the ID space.
type bucket struct {
	entries []Entry
	mu		sync.Mutex
}

// add inserts or refreshes an entry in the bucket.
// If the bucket is full, the oldest entry is evicted to make room.
// In production Kademlia you would ping the oldest entry first —
// we evict directly since we have no liveness check yet.

func (b *bucket) add(e Entry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// if already present, move to the end (most recently seen)
	for i, existing := range b.entries {
		if existing.ID == e.ID {
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			b.entries = append(b.entries, e)
			return
		}
	}

	if len(b.entries) >= K {
		b.entries = b.entries[1:]
	}

	b.entries = append(b.entries, e)
}

// closest return up to n entries from this bucket.
func (b *bucket) closest(n int) []Entry {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.entries) <= n {
		result := make([]Entry, len(b.entries))
		copy(result, b.entries)
		return result
	}
	result := make([]Entry, n)
	copy(result, b.entries[len(b.entries)-n:])
	return result
}

// RoutingTable manages 256 k-buckets, one per bit position.
type RoutingTable struct {
	self	common.NodeID
	buckets	[256]bucket
}

// NewRoutingTable creates a routing table for a node with the given ID.
func NewRoutingTable(self common.NodeID) *RoutingTable {
	return &RoutingTable{self: self}
}

// bucketIndex returns which bucket an ID belongs to.
// It is the index of the highest differing bit between self and id.
// Two identical IDs would return -1 - we never add ourselves.
func (rt *RoutingTable) bucketIndex(id common.NodeID) int {
	dist := rt.self.XORDistance(id)
	for i := 0; i < 256; i++ {
		byteIndex := i / 8
		bitIndex := 7 - (i % 8)
		if(dist[byteIndex]>>bitIndex)&1 == 1 {
			return 255 - i
		}
	}
	return -1 // identical ID - never happens in practice
}

// Add inserts a node into the appropiate bucket.
// Silently ignores attempts to add ourselves.
func (rt *RoutingTable) Add(e Entry) {
	if e.ID == rt.self {
		return
	}
	idx := rt.bucketIndex(e.ID)
	if idx < 0 {
		return
	}
	rt.buckets[idx].add(e)
}

// Closest returns the K entries whose IDs are closest target,
// sorted by XOR distance ascending.
func (rt *RoutingTable) Closest(target common.NodeID, n int) []Entry {
	// Collect candidates from all buckets
	var candidates []Entry
	for i := range rt.buckets {
		candidates = append(candidates, rt.buckets[i].closest(K)...)
	}

	// Sort by XOR distance to target
	sort.Slice(candidates, func(i, j int) bool {
		di := target.XORDistance(candidates[i].ID)
		dj := target.XORDistance(candidates[j].ID)
		for k := 0; k < 32; k++ {
			if di[k] != dj[k] {
				return di[k] < dj[k]
			}
		}
		return false
	})

	if len(candidates) <= n {
		return candidates
	}
	return candidates[:n]
}

// Size returns the total number of entries across all buckets.
func (rt *RoutingTable) Size() int {
	total := 0
	for i := range rt.buckets {
		rt.buckets[i].mu.Lock()
		total += len(rt.buckets[i].entries)
		rt.buckets[i].mu.Unlock()
	}
	return total
}