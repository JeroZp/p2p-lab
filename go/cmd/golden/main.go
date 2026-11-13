// Command golden writes reference wire messages produced by the Go
// implementation, so other implementations can be tested against
// real bytes instead of assumptions.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/JeroZp/p2p-lab/internal/common"
	"github.com/JeroZp/p2p-lab/internal/core"
	"github.com/JeroZp/p2p-lab/internal/dht"
	"github.com/JeroZp/p2p-lab/internal/gossip"
)

func main() {
	out := flag.String("out", "../python/tests/golden", "output directory")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}

	// Fixed IDs so the Python tests can assert exact values
	var id, target common.NodeID
	for i := range id {
		id[i] = byte(i)           // 0, 1, 2, ... 31
		target[i] = byte(255 - i) // 255, 254, ... 224
	}

	payload := func(v any) json.RawMessage {
		b, err := json.Marshal(v)
		if err != nil {
			log.Fatal(err)
		}
		return b
	}

	msgs := map[string]core.Message{
		"handshake": {Type: core.TypeHandshake, From: id, ID: "handshake"},
		"heartbeat": {Type: core.TypeHeartbeat},
		"gossip": {Type: core.TypeGossip, From: id, ID: "g-1",
			Payload: payload(gossip.GossipPayload{TTL: 5, Data: []byte("hello"), SentAt: 1700000000000000000})},
		"find_node": {Type: "dht_find_node", From: id, ID: "fn-1",
			Payload: payload(dht.FindNodePayload{Target: target, Addr: "node1:7000"})},
		"find_reply": {Type: "dht_find_reply", From: id, ID: "fn-1",
			Payload: payload(dht.FindReplyPayload{Nodes: []dht.Entry{{ID: target, Addr: "node2:7000"}}})},
		"store": {Type: "dht_store", From: id, ID: "st-1",
			Payload: payload(dht.StorePayload{Key: "k", Value: []byte("v")})},
		"value_reply": {Type: "dht_value_reply", From: id, ID: "fv-1",
			Payload: payload(dht.ValueReplyPayload{Values: [][]byte{[]byte("v1"), []byte("v2")}})},
	}

	for name, m := range msgs {
		var buf bytes.Buffer
		if err := core.WriteMessage(&buf, m); err != nil {
			log.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(*out, name+".bin"), buf.Bytes(), 0o644); err != nil {
			log.Fatal(err)
		}
	}

	// A real Go key file plus the NodeID Go derives from it
	_, nodeID, err := common.LoadOrCreate(filepath.Join(*out, "go_identity.bin"))
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(*out, "node_id.txt"), []byte(nodeID.String()+"\n"), 0o644); err != nil {
		log.Fatal(err)
	}

	log.Printf("wrote %d messages and an identity to %s", len(msgs), *out)
}