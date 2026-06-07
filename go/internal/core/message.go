package core

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/JeroZp/p2p-lab/internal/common"
)

// MessageType identifies what a message contains so the receiver knows which handler to dispatch it to.
type MessageType string

const (
	TypePing      MessageType = "ping"
	TypePong      MessageType = "pong"
	TypeHandshake MessageType = "handshake"
	TypeGossip	  MessageType = "gossip"
)

// Mesagge is the envelope for every message exchanged between nodes.
// Every module (gossip, DHT, transfer) wraps its data in this envelope.
type Message struct {
	Type    MessageType     `json:"type"`
	From    common.NodeID   `json:"from"`
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// maxMessageSize caps incoming messages to prevent memory exhaustion
// from a malformed or malicious peer sending a huge length prefix.
const maxMessageSize = 10 * 1024 * 1024 // 10MB

// WriteMessage writes a length-prefixed JSON message to a writer.
// Format: [4 bytes big-endian length][JSON payload]
func WriteMessage(w io.Writer, msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	length := uint32(len(data))
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return fmt.Errorf("write length prefix: %w", err)
	}

	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}

	return nil
}

// ReadMessage reads a length-prefixed JSON message to a reader.
// It blocks until a complete message is available.
func ReadMessage(r io.Reader) (Message, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return Message{}, fmt.Errorf("read length prefix: %w", err)
	}

	if length > maxMessageSize {
		return Message{}, fmt.Errorf("message too large: %d bytes", length)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return Message{}, fmt.Errorf("read payload: %w", err)
	}

	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return Message{}, fmt.Errorf("unmarshal message: %w", err)
	}

	return msg, nil
}
