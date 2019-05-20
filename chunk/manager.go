package chunk

type ChunkManager struct {
	ChunkPaths []string
	Chunks     map[string]*Chunk
}
