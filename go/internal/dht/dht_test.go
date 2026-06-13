package dht

import (
	"fmt"
	"testing"
	"time"

	"github.com/JeroZp/p2p-lab/internal/core"
	"github.com/JeroZp/p2p-lab/internal/common"
)

// buildDHTCluster creates n DHT nodes connected in a line.
func buildDHTCluster(t *testing.T, n int) ([]*core.Node, []*Node) {
	t.Helper()

	coreNodes := make([]*core.Node, n)
	dhtNodes := make([]*Node, n)

	// Create and start all nodes
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 19000+i)
		node, err := core.NewNode(fmt.Sprintf("%s/node%d.key", t.TempDir(), i))
		if err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
		if err := node.Listen(addr); err != nil {
			t.Fatalf("listen node %d: %v", i, err)
		}
		coreNodes[i] = node
		dhtNodes[i] = NewDHTNode(node, addr)
		dhtNodes[i].Start()
	}

	// Fully connect — every node dials every other node
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if err := coreNodes[i].Dial(coreNodes[j].Addr()); err != nil {
				t.Fatalf("dial %d→%d: %v", i, j, err)
			}
			// Populate routing tables in both directions
			dhtNodes[i].table.Add(Entry{
				ID:   coreNodes[j].ID,
				Addr: coreNodes[j].Addr(),
			})
			dhtNodes[j].table.Add(Entry{
				ID:   coreNodes[i].ID,
				Addr: coreNodes[i].Addr(),
			})
		}
	}

	// Give handshakes time to complete
	time.Sleep(200 * time.Millisecond)

	t.Cleanup(func() {
		for i := range dhtNodes {
			dhtNodes[i].Stop()
			coreNodes[i].Shutdown()
		}
	})

	return coreNodes, dhtNodes
}

func TestRoutingTable_AddAndClosest(t *testing.T) {
	kp1, _ := common.GenerateKeyPair()
	kp2, _ := common.GenerateKeyPair()
	kp3, _ := common.GenerateKeyPair()

	self := common.DeriveNodeID(kp1.Public)
	other1 := common.DeriveNodeID(kp2.Public)
	other2 := common.DeriveNodeID(kp3.Public)

	rt := NewRoutingTable(self)
	rt.Add(Entry{ID: other1, Addr: "127.0.0.1:7001"})
	rt.Add(Entry{ID: other2, Addr: "127.0.0.1:7002"})

	closest := rt.Closest(self, K)
	if len(closest) != 2 {
		t.Errorf("expected 2 entries, got %d", len(closest))
	}
}

func TestXORDistance_Ordering(t *testing.T) {
	kp1, _ := common.GenerateKeyPair()
	kp2, _ := common.GenerateKeyPair()
	kp3, _ := common.GenerateKeyPair()

	id1 := common.DeriveNodeID(kp1.Public)
	id2 := common.DeriveNodeID(kp2.Public)
	id3 := common.DeriveNodeID(kp3.Public)

	d12 := id1.XORDistance(id2)
	d13 := id1.XORDistance(id3)

	// Distance is not zero for different IDs
	var zero common.NodeID
	if d12 == zero {
		t.Error("XOR distance between different nodes should not be zero")
	}
	if d13 == zero {
		t.Error("XOR distance between different nodes should not be zero")
	}
}

func TestDHT_StoreAndRetrieve(t *testing.T) {
	_, dhtNodes := buildDHTCluster(t, 5)

	// Store from node 0
	if err := dhtNodes[0].Store("hello", []byte("world")); err != nil {
		t.Fatalf("Store: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Retrieve from node 4 — the far end of the cluster
	value, found := dhtNodes[4].FindValue("hello")
	if !found {
		t.Fatal("FindValue: key not found")
	}
	if string(value) != "world" {
		t.Errorf("got %q want %q", value, "world")
	}
}

func TestDHT_MissingKey(t *testing.T) {
	_, dhtNodes := buildDHTCluster(t, 3)

	_, found := dhtNodes[0].FindValue("nonexistent-key")
	if found {
		t.Error("expected key not found, but FindValue returned true")
	}
}