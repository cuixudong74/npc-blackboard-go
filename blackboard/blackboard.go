package blackboard

import "sync"

// Blackboard holds the latest committed snapshot.
//
// Reads are lock-free after grabbing the immutable snapshot pointer; writes
// swap in a new snapshot atomically. In-flight readers keep reading the old
// snapshot for the rest of their decision, so they never observe a partial
// commit.
type Blackboard struct {
	mu  sync.RWMutex
	cur *Snapshot
}

// NewBlackboard returns an empty blackboard at tick 0.
func NewBlackboard() *Blackboard {
	return &Blackboard{cur: &Snapshot{tick: 0, data: map[Namespace]map[string]Fact{}}}
}

// Snapshot returns the current immutable snapshot.
func (b *Blackboard) Snapshot() *Snapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.cur
}

// commit atomically installs the next snapshot.
func (b *Blackboard) commit(next *Snapshot) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cur = next
}

// Commit is the package-internal commit entry point used by the runtime.
func (b *Blackboard) Commit(next *Snapshot) { b.commit(next) }
