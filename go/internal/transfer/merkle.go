package transfer

import (
	"crypto/sha256"
	"fmt"
)

// MerkleTree stores every level of the tree.
// levels[0] are the leaves (chunk hashes); the last level holds only the root.
type MerkleTree struct {
	levels [][][32]byte
}

// hashPair computes SHA256(left || right)
func hashPair(left, right [32]byte) [32]byte {
	var buf [64]byte
	copy(buf[:32], left[:])
	copy(buf[32:], right[:])
	return sha256.Sum256(buf[:])
}

// NewMerkleTree builds the tree bottom-up from the leaf hashes.
// If a level has an odd number of nodes, the last one is paired with itself.
func NewMerkleTree(leaves [][32]byte) (*MerkleTree, error) {
	if len(leaves) == 0 {
		return nil, fmt.Errorf("cannot build Merkle tree with zero leaves")
	}

	level := make([][32]byte, len(leaves))
	copy(level, leaves)
	levels := [][][32]byte{level}

	for len(level) > 1 {
		var next [][32]byte
		for i := 0; i < len(level); i += 2 {
			left := level[i]
			right := left // duplicate if odd
			if i+1 < len(level) {
				right = level[i+1]
			}
			next = append(next, hashPair(left, right))
		}
		levels = append(levels, next)
		level = next
	}
	return &MerkleTree{levels: levels}, nil
}

// Root returns the single hash that commits to every chunk.
func (t *MerkleTree) Root() [32]byte {
	return t.levels[len(t.levels)-1][0]
}

// ProofStep is one sibling hash on the path from a leaf to the root.
type ProofStep struct {
	Hash   [32]byte `json:"hash"`
	IsLeft bool     `json:"is_left"` // true if the sibling sits on the left
}

// Proof returns the sibling hashes needed to prove leaf `index` is in the tree.
func (t *MerkleTree) Proof(index int) ([]ProofStep, error) {
	if index < 0 || index >= len(t.levels[0]) {
		return nil, fmt.Errorf("leaf index %d out of range", index)
	}

	var proof []ProofStep
	for _, level := range t.levels[:len(t.levels)-1] {
		sibling := index ^ 1 // flips the last bit: 4↔5, 6↔7
		if sibling >= len(level) {
			sibling = index // odd level: node was paired with itself
		}
		proof = append(proof, ProofStep{
			Hash:   level[sibling],
			IsLeft: sibling < index,
		})
		index /= 2 // move up one level
	}
	return proof, nil
}

// VerifyProof recomputes the root from a leaf and its proof.
// Needs no access to the tree, only the trusted root hash.
func VerifyProof(leaf [32]byte, proof []ProofStep, root [32]byte) bool {
	h := leaf
	for _, step := range proof {
		if step.IsLeft {
			h = hashPair(step.Hash, h)
		} else {
			h = hashPair(h, step.Hash)
		}
	}
	return h == root
}