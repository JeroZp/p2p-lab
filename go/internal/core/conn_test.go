package core

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/JeroZp/p2p-lab/internal/common"
)

// setupPair creates two connected net.Conn instances using an
// in-process TCP listener — no external network needed.
func setupPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Accept in a goroutine so Dial doesn't block
	connCh := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		connCh <- c
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	server := <-connCh
	ln.Close()

	t.Cleanup(func() {
		client.Close()
		server.Close()
	})

	return client, server
}

func TestConn_SendReceive(t *testing.T) {
	kp, _ := common.GenerateKeyPair()
	peerID := common.DeriveNodeID(kp.Public)

	clientConn, serverConn := setupPair(t)

	ctx := context.Background()
	client := newConn(ctx, peerID, clientConn)
	server := newConn(ctx, peerID, serverConn)

	client.start()
	server.start()

	// Send a message from client to server
	sent := Message{Type: TypePing, From: peerID, ID: "test-001"}
	if err := client.Send(sent); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Receive on the server side with a timeout
	select {
	case received := <-server.inbound:
		if received.ID != sent.ID {
			t.Errorf("ID: got %q want %q", received.ID, sent.ID)
		}
		if received.Type != sent.Type {
			t.Errorf("Type: got %q want %q", received.Type, sent.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	client.close()
	server.close()
}

func TestConn_StateTransitions(t *testing.T) {
	kp, _ := common.GenerateKeyPair()
	peerID := common.DeriveNodeID(kp.Public)

	clientConn, serverConn := setupPair(t)
	defer serverConn.Close()

	ctx := context.Background()
	c := newConn(ctx, peerID, clientConn)
	c.start()

	if c.State() != StateConnected {
		t.Errorf("expected StateConnected, got %v", c.State())
	}

	c.close()

	if c.State() != StateDead {
		t.Errorf("expected StateDead after close, got %v", c.State())
	}
}

func TestConn_SendAfterClose(t *testing.T) {
	kp, _ := common.GenerateKeyPair()
	peerID := common.DeriveNodeID(kp.Public)

	clientConn, serverConn := setupPair(t)
	defer serverConn.Close()

	ctx := context.Background()
	c := newConn(ctx, peerID, clientConn)
	c.start()
	c.close()

	err := c.Send(Message{Type: TypePing, From: peerID, ID: "after-close"})
	if err == nil {
		t.Error("expected error sending on closed connection, got nil")
	}
}