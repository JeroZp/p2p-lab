package core

import (
	"testing"
	"time"
)

func TestNode_ConnectAndExchangeMessages(t *testing.T) {
	// Create two nodes with temporary key paths
	nodeA, err := NewNode(t.TempDir() + "/a.key")
	if err != nil {
		t.Fatal(err)
	}
	nodeB, err := NewNode(t.TempDir() + "/b.key")
	if err != nil {
		t.Fatal(err)
	}

	// Node B listens
	if err := nodeB.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	addr := nodeB.listener.Addr().String()

	// Node A dials Node B
	if err := nodeA.Dial(addr); err != nil {
		t.Fatal(err)
	}

	// Wait for handshake to complete
	time.Sleep(100 * time.Millisecond)

	// Verify they know about each other
	if len(nodeA.Peers()) == 0 {
		t.Fatal("nodeA has no peers after connecting")
	}
	if len(nodeB.Peers()) == 0 {
		t.Fatal("nodeB has no peers after connecting")
	}

	// Node A sends a ping to Node B
	ping := Message{
		Type: TypePing,
		From: nodeA.ID,
		ID:   "ping-001",
	}
	if err := nodeA.Send(nodeB.ID, ping); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Node B should receive it
	select {
	case msg := <-nodeB.Inbound:
		if msg.ID != ping.ID {
			t.Errorf("ID: got %q want %q", msg.ID, ping.ID)
		}
		if msg.Type != TypePing {
			t.Errorf("Type: got %q want %q", msg.Type, TypePing)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message on nodeB")
	}

	nodeA.Shutdown()
	nodeB.Shutdown()
}

func TestNode_ShutdownCleanly(t *testing.T) {
	node, err := NewNode(t.TempDir() + "/node.key")
	if err != nil {
		t.Fatal(err)
	}

	if err := node.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	// Should return without hanging
	done := make(chan struct{})
	go func() {
		node.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// clean shutdown
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not complete in time")
	}
}

func TestNode_RemovesPeerWhenConnectionDies(t *testing.T) {
	nodeA, err := NewNode(t.TempDir() + "/a.key")
	if err != nil {
		t.Fatal(err)
	}
	nodeB, err := NewNode(t.TempDir() + "/b.key")
	if err != nil {
		t.Fatal(err)
	}
	defer nodeA.Shutdown()

	if err := nodeB.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	if err := nodeA.Dial(nodeB.Addr()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if len(nodeA.Peers()) != 1 {
		t.Fatalf("expected 1 peer before B dies, got %d", len(nodeA.Peers()))
	}

	// B goes away — A must notice and clean up
	nodeB.Shutdown()
	time.Sleep(300 * time.Millisecond)

	if n := len(nodeA.Peers()); n != 0 {
		t.Errorf("expected 0 peers after B died, got %d", n)
	}
}