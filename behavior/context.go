package behavior

import (
	"hash/fnv"
	"math/rand"
)

// NewFrameRNG derives a deterministic RNG for an NPC on a frame.
// The derivation is order-independent: no matter how NPCs are scheduled,
// (seed, frame, npcID) always yields the same stream.
func NewFrameRNG(seed int64, frame int64, npcID string) *DeterministicRNG {
	h := fnv.New64a()
	_, _ = h.Write([]byte(npcID))
	return &DeterministicRNG{r: rand.New(rand.NewSource(seed ^ int64(h.Sum64()) ^ frame*0x9e3779b9))}
}

// Intn returns a deterministic int in [0,n).
func (d *DeterministicRNG) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return d.r.Intn(n)
}

// Float64 returns a deterministic float in [0,1).
func (d *DeterministicRNG) Float64() float64 { return d.r.Float64() }

// Shuffle deterministically shuffles a slice.
func (d *DeterministicRNG) Shuffle(n int, swap func(i, j int)) { d.r.Shuffle(n, swap) }
