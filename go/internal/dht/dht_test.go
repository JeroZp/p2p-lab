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
	values, found := dhtNodes[4].FindValue("hello")
	if !found {
		t.Fatal("FindValue: key not found")
	}
	if len(values) != 1 || string(values[0]) != "world" {
		t.Errorf("got %q, want [\"world\"]", values)
	}
}

func TestDHT_MissingKey(t *testing.T) {
	_, dhtNodes := buildDHTCluster(t, 3)

	_, found := dhtNodes[0].FindValue("nonexistent-key")
	if found {
		t.Error("expected key not found, but FindValue returned true")
	}
}

func TestFindNode_NoDuplicatesOrSelf(t *testing.T) {
	coreNodes, dhtNodes := buildDHTCluster(t, 5)
	self := coreNodes[0].ID

	results := dhtNodes[0].FindNode(self)

	seen := make(map[common.NodeID]bool)
	for _, e := range results {
		if e.ID == self {
			t.Error("FindNode returned the querying node itself")
		}
		if seen[e.ID] {
			t.Errorf("duplicate entry: %s", e.ID)
		}
		seen[e.ID] = true
	}

	// 5-node cluster: exactly 4 other nodes exist
	if len(results) != 4 {
		t.Errorf("expected 4 unique nodes, got %d", len(results))
	}
}

func TestFindNode_DoesNotWaitForUnresponsivePeer(t *testing.T) {
	coreNodes, dhtNodes := buildDHTCluster(t, 4)

	// A connected peer that never answers DHT requests
	silent, err := core.NewNode(t.TempDir() + "/silent.key")
	if err != nil {
		t.Fatal(err)
	}
	if err := silent.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer silent.Shutdown()

	for i, d := range dhtNodes {
		if err := coreNodes[i].Dial(silent.Addr()); err != nil {
			t.Fatal(err)
		}
		d.AddNode(Entry{ID: silent.ID, Addr: silent.Addr()})
	}
	time.Sleep(200 * time.Millisecond)

	// Looking up the silent node's own ID guarantees it is queried
	start := time.Now()
	dhtNodes[0].FindNode(silent.ID)
	elapsed := time.Since(start)

	t.Logf("lookup took %v", elapsed)
	if elapsed > time.Second {
		t.Errorf("lookup waited for the unresponsive peer (%v)", elapsed)
	}
}

func TestDHT_RoutingTableFilledOnConnect(t *testing.T) {
	a, err := core.NewNode(t.TempDir() + "/a.key")
	if err != nil {
		t.Fatal(err)
	}
	b, err := core.NewNode(t.TempDir() + "/b.key")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Shutdown()
	defer b.Shutdown()

	da := NewDHTNode(a, "a")
	db := NewDHTNode(b, "b")
	da.Start()
	db.Start()
	defer da.Stop()
	defer db.Stop()

	if err := b.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	if err := a.Dial(b.Addr()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for da.table.Size() < 1 || db.table.Size() < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("routing tables not filled on connect: a=%d b=%d",
				da.table.Size(), db.table.Size())
		}
		time.Sleep(20 * time.Millisecond)
	}
}