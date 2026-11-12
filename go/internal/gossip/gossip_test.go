package gossip

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/JeroZp/p2p-lab/internal/core"
)

// buildCluster creates n nodes connected in a line:
// node0 — node1 — node2 — ... — nodeN
// Not a full mesh, but enough to test multi-hop propagation.
func buildCluster(t *testing.T, n int) ([]*core.Node, []*Engine) {
	t.Helper()

	nodes := make([]*core.Node, n)
	engines := make([]*Engine, n)

	for i := 0; i < n; i++ {
		node, err := core.NewNode(fmt.Sprintf("%s/node%d.key", t.TempDir(), i))
		if err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
		if err := node.Listen("127.0.0.1:0"); err != nil {
			t.Fatalf("listen node %d: %v", i, err)
		}
		nodes[i] = node
		engines[i] = NewEngine(node)
		engines[i].Start()
	}

	// Connect each node to the next one in line
	for i := 0; i < n-1; i++ {
		addr := nodes[i+1].Addr()
		if err := nodes[i].Dial(addr); err != nil {
			t.Fatalf("dial node %d → %d: %v", i, i+1, err)
		}
	}

	// Give handshakes time to complete
	time.Sleep(100 * time.Millisecond)

	t.Cleanup(func() {
		for i := range engines {
			engines[i].Stop()
			nodes[i].Shutdown()
		}
	})

	return nodes, engines
}

func TestGossip_PropagatestoAllNodes(t *testing.T) {
	nodes, engines := buildCluster(t, 5)

	// Inject a message at node 0
	data := []byte("hello from node 0")
	if err := engines[0].Broadcast(data); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}

	// Every other node should receive it
	for i := 1; i < len(nodes); i++ {
		select {
		case received := <-engines[i].Received:
			if string(received) != string(data) {
				t.Errorf("node %d: got %q want %q", i, received, data)
			}
		case <-time.After(3 * time.Second):
			t.Errorf("node %d: timeout waiting for gossip message", i)
		}
	}
}

func TestGossip_DeduplicatesMessages(t *testing.T) {
	_, engines := buildCluster(t, 3)

	// Manually mark a message as seen on engine 1
	engines[1].markSeen("duplicate-id")

	// Inject the same message ID via handle directly
	msg := core.Message{
		Type: core.TypeGossip,
		From: engines[0].node.ID,
		ID:   "duplicate-id",
		Payload: func() []byte {
			p := GossipPayload{TTL: 3, Data: []byte("test")}
			b, _ := json.Marshal(p)
			return b
		}(),
	}

	engines[1].handle(msg)

	// Received channel should be empty — message was deduplicated
	select {
	case <-engines[1].Received:
		t.Error("expected message to be deduplicated, but it was delivered")
	case <-time.After(200 * time.Millisecond):
		// correct — nothing delivered
	}
}

func TestGossip_TTLExpiry(t *testing.T) {
	_, engines := buildCluster(t, 3)

	// Message with TTL=1 should be delivered but not forwarded further
	payload := GossipPayload{TTL: 1, Data: []byte("ttl-test")}
	payloadBytes, _ := json.Marshal(payload)

	msg := core.Message{
		Type:    core.TypeGossip,
		From:    engines[0].node.ID,
		ID:      "ttl-test-id",
		Payload: payloadBytes,
	}

	engines[1].handle(msg)

	// Should be delivered to engine 1
	select {
	case received := <-engines[1].Received:
		if string(received) != "ttl-test" {
			t.Errorf("got %q want %q", received, "ttl-test")
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("timeout: message should have been delivered")
	}

	// Engine 2 should NOT receive it — TTL expired at engine 1
	select {
	case <-engines[2].Received:
		t.Error("expected TTL to stop propagation at node 1")
	case <-time.After(500 * time.Millisecond):
		// correct — TTL stopped it
	}
}