package lsm

import "sync"

type FileStore struct {
	mu sync.RWMutex

	currentGeneration int
}

func (f *FileStore) NextGeneration() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.currentGeneration++
	return f.currentGeneration
}
