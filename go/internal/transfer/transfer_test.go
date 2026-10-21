package transfer

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"os"
	"testing"
)

func TestChunkReader_SplitsCorrectly(t *testing.T) {
	data := make([]byte, 25)
	rand.Read(data)

	chunks, err := ChunkReader(bytes.NewReader(data), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	if len(chunks[2].Data) != 5 {
		t.Errorf("last chunk: expected 5 bytes, got %d", len(chunks[2].Data))
	}
	for i, c := range chunks {
		if c.Index != i {
			t.Errorf("chunk %d has index %d", i, c.Index)
		}
		if !c.Verify() {
			t.Errorf("chunk %d failed verification", i)
		}
	}
}

func TestChunkFile_RoundTrip(t *testing.T) {
	data := make([]byte, 3*1024*1024+123) // 3 MB + a bit
	rand.Read(data)

	path := t.TempDir() + "/test.bin"
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}

	chunks, err := ChunkFile(path, DefaultChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(chunks))
	}

	// Shuffle order to simulate chunks arriving from different peers
	chunks[0], chunks[3] = chunks[3], chunks[0]

	out, err := Reassemble(chunks)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, data) {
		t.Error("reassembled file does not match original")
	}
}

func TestReassemble_DetectsTampering(t *testing.T) {
	chunks, _ := ChunkReader(bytes.NewReader([]byte("hello p2p world!")), 4)
	chunks[1].Data[0] ^= 0xFF // flip bits in one chunk

	if _, err := Reassemble(chunks); err == nil {
		t.Error("expected tampered chunk to fail verification")
	}
}

func TestReassemble_DetectsMissingChunk(t *testing.T) {
	chunks, _ := ChunkReader(bytes.NewReader([]byte("hello p2p world!")), 4)
	chunks = append(chunks[:1], chunks[2:]...) // drop chunk 1

	if _, err := Reassemble(chunks); err == nil {
		t.Error("expected missing chunk to be detected")
	}
}

func TestMerkle_SingleLeafRootIsLeaf(t *testing.T) {
	leaf := [32]byte{1, 2, 3}
	tree, err := NewMerkleTree([][32]byte{leaf})
	if err != nil {
		t.Fatal(err)
	}
	if tree.Root() != leaf {
		t.Error("root of single-leaf tree should equal the leaf")
	}
}

func TestMerkle_TamperingChangesRoot(t *testing.T) {
	chunks, _ := ChunkReader(bytes.NewReader(make([]byte, 100)), 10)
	tree1, _ := NewMerkleTree(Hashes(chunks))

	chunks[7].Data[0] = 0xFF
	chunks[7].Hash = [32]byte{} // recomputed below
	chunks[7].Hash = sha256Sum(chunks[7].Data)
	tree2, _ := NewMerkleTree(Hashes(chunks))

	if tree1.Root() == tree2.Root() {
		t.Error("changing one chunk should change the root")
	}
}

func TestMerkle_ProofsVerifyForEveryLeaf(t *testing.T) {
	// Test odd and even sizes — odd sizes exercise the "pair with itself" path
	for n := 1; n <= 9; n++ {
		data := make([]byte, n*10)
		rand.Read(data)
		chunks, _ := ChunkReader(bytes.NewReader(data), 10)
		leaves := Hashes(chunks)
		tree, _ := NewMerkleTree(leaves)

		for i, leaf := range leaves {
			proof, err := tree.Proof(i)
			if err != nil {
				t.Fatalf("n=%d leaf=%d: %v", n, i, err)
			}
			if !VerifyProof(leaf, proof, tree.Root()) {
				t.Errorf("n=%d leaf=%d: valid proof rejected", n, i)
			}
		}
	}
}

func TestMerkle_WrongLeafFailsProof(t *testing.T) {
	data := make([]byte, 80)
	rand.Read(data)
	chunks, _ := ChunkReader(bytes.NewReader(data), 10)
	tree, _ := NewMerkleTree(Hashes(chunks))

	proof, _ := tree.Proof(3)
	fake := [32]byte{0xde, 0xad}
	if VerifyProof(fake, proof, tree.Root()) {
		t.Error("forged leaf should not verify")
	}
}

// Small helper
func sha256Sum(b []byte) [32]byte { return sha256.Sum256(b) }