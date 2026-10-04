package behavior

import (
	"context"
	"testing"

	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// stubBehavior is a deterministic test behavior.
type stubBehavior struct {
	id         string
	priority   int
	applicable bool
	cost       float64
	writes     []blackboard.WriteOp
	err        error
}

func (s stubBehavior) ID() string                            { return s.id }
func (s stubBehavior) Priority() int                        { return s.priority }
func (s stubBehavior) Applicable(ctx *Context) bool         { return s.applicable }
func (s stubBehavior) Cost(ctx *Context) float64            { return s.cost }
func (s stubBehavior) Execute(ctx *Context) ([]blackboard.WriteOp, error) {
	return s.writes, s.err
}

func ctxForTest(frame int64) *Context {
	snap := blackboard.NewSnapshot(frame, nil)
	return &Context{
		Frame:  frame,
		NPCID:  "npc",
		View:   blackboard.NewView(snap, blackboard.NewGrantSet()),
		Events: nil,
		RNG:    NewFrameRNG(1, frame, "npc"),
		Cancel: context.Background(),
	}
}

func TestArbitratePriorityWins(t *testing.T) {
	behaviors := []Behavior{
		stubBehavior{id: "low", priority: 1, applicable: true, cost: 1},
		stubBehavior{id: "high", priority: 10, applicable: true, cost: 100},
	}
	d := Arbitrate("npc", 1, "", behaviors, ctxForTest(1))
	if d.WinnerID != "high" {
		t.Fatalf("winner = %q, want high (priority)", d.WinnerID)
	}
	if len(d.Conflicts) != 1 || d.Conflicts[0].LoserID != "low" {
		t.Fatalf("conflicts = %+v, want low loser", d.Conflicts)
	}
	if d.Conflicts[0].Rule != "higher priority" {
		t.Fatalf("rule = %q", d.Conflicts[0].Rule)
	}
}

func TestArbitrateCostTieBreak(t *testing.T) {
	behaviors := []Behavior{
		stubBehavior{id: "expensive", priority: 5, applicable: true, cost: 10},
		stubBehavior{id: "cheap", priority: 5, applicable: true, cost: 1},
	}
	d := Arbitrate("npc", 1, "", behaviors, ctxForTest(1))
	if d.WinnerID != "cheap" {
		t.Fatalf("winner = %q, want cheap (lower cost)", d.WinnerID)
	}
	if d.Conflicts[0].Rule != "lower cost" {
		t.Fatalf("rule = %q", d.Conflicts[0].Rule)
	}
}

func TestArbitrateLexicographicTieBreak(t *testing.T) {
	behaviors := []Behavior{
		stubBehavior{id: "zeta", priority: 5, applicable: true, cost: 1},
		stubBehavior{id: "alpha", priority: 5, applicable: true, cost: 1},
	}
	d := Arbitrate("npc", 1, "", behaviors, ctxForTest(1))
	if d.WinnerID != "alpha" {
		t.Fatalf("winner = %q, want alpha (lexicographic tie-break)", d.WinnerID)
	}
	if d.Conflicts[0].Rule != "lexicographic tie-break" {
		t.Fatalf("rule = %q", d.Conflicts[0].Rule)
	}
}

func TestArbitrateIdle(t *testing.T) {
	behaviors := []Behavior{
		stubBehavior{id: "a", priority: 1, applicable: false},
		stubBehavior{id: "b", priority: 1, applicable: false},
	}
	d := Arbitrate("npc", 1, "a", behaviors, ctxForTest(1))
	if !d.IsIdle() {
		t.Fatalf("expected idle, got winner %q", d.WinnerID)
	}
	if d.PreemptedFrom != "a" {
		t.Fatalf("preempted from = %q, want a", d.PreemptedFrom)
	}
}

func TestArbitrateNoPreemptionWhenSameWinner(t *testing.T) {
	behaviors := []Behavior{
		stubBehavior{id: "a", priority: 1, applicable: true, cost: 1},
	}
	d := Arbitrate("npc", 2, "a", behaviors, ctxForTest(2))
	if d.WinnerID != "a" {
		t.Fatalf("winner = %q", d.WinnerID)
	}
	if d.PreemptedFrom != "" {
		t.Fatalf("should not preempt same winner, got %q", d.PreemptedFrom)
	}
}

func TestArbitratePreemption(t *testing.T) {
	behaviors := []Behavior{
		stubBehavior{id: "a", priority: 1, applicable: true, cost: 1},
		stubBehavior{id: "b", priority: 5, applicable: true, cost: 1},
	}
	d := Arbitrate("npc", 2, "a", behaviors, ctxForTest(2))
	if d.WinnerID != "b" {
		t.Fatalf("winner = %q, want b", d.WinnerID)
	}
	if d.PreemptedFrom != "a" {
		t.Fatalf("preempted from = %q, want a", d.PreemptedFrom)
	}
}

func TestArbitrateExecuteError(t *testing.T) {
	boom := stubBehavior{id: "boom", priority: 10, applicable: true, cost: 1, err: errBoom}
	d := Arbitrate("npc", 1, "", []Behavior{boom}, ctxForTest(1))
	if d.Err != errBoom {
		t.Fatalf("err = %v, want errBoom", d.Err)
	}
}

func TestDeterministicRNG(t *testing.T) {
	a := NewFrameRNG(42, 7, "npc-x")
	b := NewFrameRNG(42, 7, "npc-x")
	for i := 0; i < 100; i++ {
		if a.Intn(1000) != b.Intn(1000) {
			t.Fatal("RNG not deterministic")
		}
	}
	c := NewFrameRNG(42, 7, "npc-y")
	if a.Intn(1000) == c.Intn(1000) {
		// different NPC should (almost certainly) differ; not a hard assert
	}
}

var errBoom = context.Canceled
