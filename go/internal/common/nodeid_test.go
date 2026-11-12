package common

import (
	"os"
	"testing"
)

func TestDeriveNodeID_IsDeterministic(t *testing.T) {
	kp, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	id1 := DeriveNodeID(kp.Public)
	id2 := DeriveNodeID(kp.Public)
	if id1 != id2 {
		t.Error("same public key produced different NodeIDs")
	}
}

func TestDeriveNodeID_DifferentKeys_DifferentIDs(t *testing.T) {
	kp1, _ := GenerateKeyPair()
	kp2, _ := GenerateKeyPair()
	id1 := DeriveNodeID(kp1.Public)
	id2 := DeriveNodeID(kp2.Public)
	if id1 == id2 {
		t.Error("different keys produced the same NodeID")
	}
}

func TestXORDistance_SameNode_IsZero(t *testing.T) {
	kp, _ := GenerateKeyPair()
	id := DeriveNodeID(kp.Public)
	dist := id.XORDistance(id)
	var zero NodeID
	if dist != zero {
		t.Error("XOR of node with itself should be zero")
	}
}

func TestLoadOrCreate_PersistsIdentity(t *testing.T) {
	path := t.TempDir() + "/node.key"

	// First call — generates and saves
	_, id1, err := LoadOrCreate(path)
	if err != nil {
		t.Fatal(err)
	}

	// Second call — loads from disk
	_, id2, err := LoadOrCreate(path)
	if err != nil {
		t.Fatal(err)
	}

	if id1 != id2 {
		t.Error("NodeID changed between restarts")
	}

	os.Remove(path)
}