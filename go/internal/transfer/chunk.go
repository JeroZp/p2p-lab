package transfer

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

// DefaultChunkSize is 1MB
const DefaultChunkSize = 1 << 20

// Chunk is one fixed-size piece of a file, identified by its content hash.
type Chunk struct {
	Index int		`json:"index"`
	Data  []byte	`json:"data"`
	Hash  [32]byte	`json:"hash"`
}

// Verify reports whether the chunk's data still matches its hash.
func (c *Chunk) Verify() bool {
	return sha256.Sum256(c.Data) == c.Hash
}

// ChunkFile opens a file and splits it into chunks
func ChunkFile(path string, chunkSize int) ([]Chunk, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return ChunkReader(f, chunkSize)
}

// ChunkReader splits any stream into chunks of chunkSize bytes
func ChunkReader(r io.Reader, chunkSize int) ([]Chunk, error) {
	if chunkSize <= 0 {
		return nil, fmt.Errorf("invalid chunk size: %d", chunkSize)
	}

	var chunks []Chunk
	buf := make([]byte, chunkSize)

	for i := 0; ; i++ {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			// Copy out of buf: buf is reused on the next iteration,
			// so storing buf[:n] directly would overwrite every chunk.
			data := make([]byte, n)
			copy(data, buf[:n])
			chunks = append(chunks, Chunk{
				Index: i,
				Hash: sha256.Sum256(data),
				Data: data,
			})
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read chunk %d: %w", i, err)
		}
	}
	return chunks, nil
}

// Hashes returns the chunk hashes in order. These are the Merkle tree leaves
func Hashes(chunks []Chunk) [][32]byte {
	out := make([][32]byte, len(chunks))
	for i, c := range chunks {
		out[i] = c.Hash
	}
	return out
}

// Reassemble sorts chunks by index, verifies each one, and joins them.
// Fails if any chunk is missing or corrupted.
func Reassemble(chunks []Chunk) ([]byte, error) {
	sorted := make([]Chunk, len(chunks))
	copy(sorted, chunks)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Index < sorted[j].Index })

	var out bytes.Buffer
	for i, c := range sorted {
		if c.Index != i {
			return nil, fmt.Errorf("missing chunk %d", i)
		}
		if !c.Verify() {
			return nil, fmt.Errorf("chunk %d failed hash verification", i)
		}
		out.Write(c.Data)
	}
	return out.Bytes(), nil
}