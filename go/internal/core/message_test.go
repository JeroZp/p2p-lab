package core

import (
	"bytes"
	"testing"

	"github.com/JeroZp/p2p-lab/internal/common"
)

func TestMessageRoundTrip(t *testing.T) {
	kp, err := common.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	id := common.DeriveNodeID(kp.Public)

	original := Message{
		Type: TypePing,
		From: id,
		ID:   "test-123",
	}

	// Write into a buffer (simulates a TCP connection)
	var buf bytes.Buffer
	if err := WriteMessage(&buf, original); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	// Read back from the same buffer
	received, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	if received.Type != original.Type {
		t.Errorf("Type: got %q want %q", received.Type, original.Type)
	}
	if received.From != original.From {
		t.Errorf("From: got %v want %v", received.From, original.From)
	}
	if received.ID != original.ID {
		t.Errorf("ID: got %q want %q", received.ID, original.ID)
	}
}

func TestReadMessage_RejectsOversizedMessage(t *testing.T) {
	var buf bytes.Buffer

	// Write a length prefix that exceeds maxMessageSize
	tooBig := uint32(maxMessageSize + 1)
	buf.Write([]byte{
		byte(tooBig >> 24),
		byte(tooBig >> 16),
		byte(tooBig >> 8),
		byte(tooBig),
	})

	_, err := ReadMessage(&buf)
	if err == nil {
		t.Error("expected error for oversized message, got nil")
	}
}

func TestWriteMessage_MultipleMessages(t *testing.T) {
	kp, _ := common.GenerateKeyPair()
	id := common.DeriveNodeID(kp.Public)

	var buf bytes.Buffer

	// Write two messages back to back
	msg1 := Message{Type: TypePing, From: id, ID: "msg-1"}
	msg2 := Message{Type: TypePong, From: id, ID: "msg-2"}

	WriteMessage(&buf, msg1)
	WriteMessage(&buf, msg2)

	// Read them back in order
	r1, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("read first message: %v", err)
	}
	r2, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("read second message: %v", err)
	}

	if r1.ID != "msg-1" {
		t.Errorf("first message ID: got %q want %q", r1.ID, "msg-1")
	}
	if r2.ID != "msg-2" {
		t.Errorf("second message ID: got %q want %q", r2.ID, "msg-2")
	}
}