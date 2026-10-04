package behavior

import (
	"context"
	"math/rand"

	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// Behavior is a candidate action an NPC can run during a decision cycle.
//
// Implementations must be deterministic given the same Context: applicability,
// cost and execution must not depend on wall-clock time, goroutine scheduling
// or map iteration order. Use Context.RNG for any randomness.
type Behavior interface {
	// ID uniquely identifies the behavior within an NPC.
	ID() string
	// Priority is the base arbitration priority; higher wins.
	Priority() int
	// Applicable reports whether the behavior's conditions hold now.
	Applicable(ctx *Context) bool
	// Cost is the execution cost (lower wins among equal priority).
	Cost(ctx *Context) float64
	// Execute produces the blackboard writes for this frame.
	// Returning an error aborts the whole decision cycle (all-or-nothing).
	Execute(ctx *Context) ([]blackboard.WriteOp, error)
}

// Context is the per-NPC, per-frame decision context.
//
// It exposes a self-consistent blackboard view (already authorized for the
// NPC), the frame's input events, a deterministic RNG and a cancellation
// context bounded by the frame timeout.
type Context struct {
	// Frame is the current decision tick (monotonic, starts at 1).
	Frame int64
	// NPCID is the deciding NPC.
	NPCID string
	// View is the authorized blackboard snapshot for this frame.
	View *blackboard.View
	// Events are the input events delivered this frame, in seq order.
	Events []Event
	// RNG is a deterministic random source derived from (seed, frame, NPC).
	RNG *DeterministicRNG
	// Cancel is canceled when the frame deadline is exceeded.
	Cancel context.Context
}

// Get is a convenience wrapper over View.Get.
func (c *Context) Get(ns blackboard.Namespace, key string) (blackboard.Fact, error) {
	return c.View.Get(ns, key)
}

// Event is an input fact delivered to behaviors in a frame.
type Event struct {
	// Seq is the globally monotonic enqueue sequence number.
	Seq uint64
	// Type is the event kind.
	Type string
	// Payload is the event payload (must be JSON-serializable for restart).
	Payload any
	// SourceTick optionally pins the event's occurrence tick (late events).
	SourceTick int64
	// DeliverTick is the frame at which the event is delivered; 0 means next.
	DeliverTick int64
}

// DeterministicRNG is a rand.Rand-like source that is safe to share and
// stable across runs: the same seed always yields the same sequence.
type DeterministicRNG struct {
	r *rand.Rand
}
