package transfer

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/JeroZp/p2p-lab/internal/core"
	"github.com/JeroZp/p2p-lab/internal/dht"
)

// buildTransferCluster creates n fully connected nodes with DHT and transfer.
func buildTransferCluster(t *testing.T, n int) []*Engine {
	t.Helper()

	nodes := make([]*core.Node, n)
	dhts := make([]*dht.Node, n)
	engines := make([]*Engine, n)

	for i := 0; i < n; i++ {
		node, err := core.NewNode(fmt.Sprintf("%s/n%d.key", t.TempDir(), i))
		if err != nil {
			t.Fatal(err)
		}
		// Port 0 = OS picks a free port. Avoids clashes with other
		// test packages, which go test runs in parallel.
		if err := node.Listen("127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
		addr := node.Addr()

		// Subscribe DHT and transfer BEFORE any connection exists
		dhts[i] = dht.NewDHTNode(node, addr)
		dhts[i].Start()
		engines[i] = NewEngine(node, dhts[i], addr)
		engines[i].Start()
		nodes[i] = node
	}

	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if err := nodes[i].Dial(nodes[j].Addr()); err != nil {
				t.Fatalf("dial %d→%d: %v", i, j, err)
			}
			dhts[i].AddNode(dht.Entry{ID: nodes[j].ID, Addr: nodes[j].Addr()})
			dhts[j].AddNode(dht.Entry{ID: nodes[i].ID, Addr: nodes[i].Addr()})
		}
	}
	time.Sleep(300 * time.Millisecond)

	t.Cleanup(func() {
		for i := range engines {
			engines[i].Stop()
			dhts[i].Stop()
			nodes[i].Shutdown()
		}
	})
	return engines
}

func TestTransfer_FetchFromRemoteNode(t *testing.T) {
	engines := buildTransferCluster(t, 6)

	// 10 MB random file, shared by node 0
	data := make([]byte, 10*1024*1024)
	rand.Read(data)
	path := t.TempDir() + "/file.bin"
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}

	m, err := engines[0].Share(path)
	if err != nil {
		t.Fatalf("Share: %v", err)
	}
	if len(m.Chunks) != 10 {
		t.Fatalf("expected 10 chunks, got %d", len(m.Chunks))
	}

	// Node 5 has nothing locally, it only knows the root hash
	start := time.Now()
	out, err := engines[5].FetchFile(m.Root)
	if err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	t.Logf("fetched 10MB in %v", time.Since(start))

	if !bytes.Equal(out, data) {
		t.Error("fetched file does not match original")
	}
}

func TestTransfer_UnknownRootFails(t *testing.T) {
	engines := buildTransferCluster(t, 3)

	var fake [32]byte
	rand.Read(fake[:])

	if _, err := engines[1].FetchFile(fake); err == nil {
		t.Error("expected error for unknown root")
	}
}