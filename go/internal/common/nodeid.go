package common

import (
	"crypto/ed25519" // the key pair generation and signing algorithm
	"crypto/rand"
	"crypto/sha256" // for hashing the public key to create the NodeID
	"encoding/hex"  // for encoding the NodeID as a hex string for logging
	"errors"
	"fmt"
	"os"            // for file operations, such as saving the private key
	"path/filepath" // and loading
)

// NodeID is a 256-bit identifier derived from an ed25519 public key.
// It is the stable identity of a node cross restarts.
type NodeID [32]byte

// KeyPair for this node.
type KeyPair struct {
	Private ed25519.PrivateKey
	Public  ed25519.PublicKey
}

// String returns the NodeID as a hex string - used in logs and wire messages.
func (n NodeID) String() string {
	return hex.EncodeToString(n[:])
}

// XORDistance computes the XOR distance between two NodeIDs. This is used in DHT routing to determine how "close" two nodes are in the ID space.
// Two identical NodeIDs XOR to all zeros (distance = 0).
func (n NodeID) XORDistance(other NodeID) NodeID {
	var result NodeID
	for i := range len(n) {
		result[i] = n[i] ^ other[i]
	}
	return result
}

// GenerateKeyPair creates a new ed25516 key pair using a secure random source.
func GenerateKeyPair() (KeyPair, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return KeyPair{}, fmt.Errorf("generate keypair: %w", err)
	}
	return KeyPair{Private: priv, Public: pub}, nil
}

// DeriveNodeID computes the NodeID from a public key.
// NodeID = SHA256(public key)
func DeriveNodeID(pub ed25519.PublicKey) NodeID {
	return NodeID(sha256.Sum256(pub))
}

// SaveKeyPair writes the private key to a file.
func SaveKeyPair(kp KeyPair, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create key directory: %w", err)
	}
	if err := os.WriteFile(path, kp.Private, 0600); err != nil {
		return fmt.Errorf("write key file: %w", err)
	}
	return nil
}

// LoadKeyPair reads a private key from disk and re-derives the public key.
func LoadKeyPair(path string) (KeyPair, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return KeyPair{}, fmt.Errorf("read key file: %w", err)
	}
	if len(data) != ed25519.PrivateKeySize {
		return KeyPair{}, fmt.Errorf("invalid key file: expected %d bytes, got %d", ed25519.PrivateKeySize, len(data))
	}
	priv := ed25519.PrivateKey(data)
	pub := priv.Public().(ed25519.PublicKey)
	return KeyPair{Private: priv, Public: pub}, nil
}

// LoadOrCreate loads a key pair from disk if it exists,
// or generates a new one and saves it if it doesn't.
// This is called once at node startup.
func LoadOrCreate(path string) (KeyPair, NodeID, error) {
	kp, err := LoadKeyPair(path)
	if errors.Is(err, os.ErrNotExist) {
		kp, err = GenerateKeyPair()
		if err != nil {
			return KeyPair{}, NodeID{}, err
		}
		if err := SaveKeyPair(kp, path); err != nil {
			return KeyPair{}, NodeID{}, err
		}
	} else if err != nil {
		return KeyPair{}, NodeID{}, err
	}

	id := DeriveNodeID(kp.Public)
	return kp, id, nil
}
