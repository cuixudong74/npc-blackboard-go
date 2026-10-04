package behavior

import (
	"fmt"
	"math"
	"sort"

	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// CostEpsilon is the cost difference below which two costs are considered
// tied. Ties are broken by lexicographic behavior ID.
const CostEpsilon = 1e-9

// Candidate is an applicable behavior competing for one NPC's execution slot.
type Candidate struct {
	Behavior Behavior
	Priority int
	Cost     float64
}

// Conflict records two applicable behaviors that competed for the slot and
// the rule that resolved them.
type Conflict struct {
	WinnerID string
	LoserID  string
	Rule     string
}

// Decision is the arbitration result for one NPC in one frame.
type Decision struct {
	NPCID         string
	Frame         int64
	WinnerID      string // empty when idle
	PreemptedFrom string // behavior ID that lost the slot this frame
	Reason        string // human-readable explanation
	Candidates    []Candidate
	Conflicts     []Conflict
	Writes        []blackboard.WriteOp
	Err           error
}

// IsIdle reports whether no behavior won the slot.
func (d *Decision) IsIdle() bool { return d.WinnerID == "" }

// Arbitrate runs the deterministic arbitration for one NPC.
//
// Rules (applied in order, first match wins):
//  1. Only applicable behaviors compete.
//  2. Higher priority wins.
//  3. Equal priority: lower cost wins.
//  4. Equal cost (within CostEpsilon): lexicographically smaller behavior ID wins.
//  5. If the winner differs from the active behavior, the active behavior is
//     preempted (PreemptedFrom is set) and the reason is recorded.
//  6. If no behavior is applicable the NPC idles and the active behavior is
//     preempted without a winner.
//
// The outcome is a pure function of (frame, activeID, behaviors, ctx); NPC
// scheduling and map iteration order cannot change it.
func Arbitrate(npcID string, frame int64, activeID string, behaviors []Behavior, ctx *Context) Decision {
	d := Decision{NPCID: npcID, Frame: frame}

	// 1. evaluate applicability in deterministic ID order
	applicable := make([]Candidate, 0, len(behaviors))
	for _, b := range sortedBehaviors(behaviors) {
		if b.Applicable(ctx) {
			applicable = append(applicable, Candidate{Behavior: b, Priority: b.Priority(), Cost: b.Cost(ctx)})
		}
	}
	d.Candidates = applicable

	if len(applicable) == 0 {
		// 6. idle: preempt the active behavior without a winner
		if activeID != "" {
			d.PreemptedFrom = activeID
			d.Reason = fmt.Sprintf("no applicable behavior; active %q idled", activeID)
		} else {
			d.Reason = "no applicable behavior; idle"
		}
		return d
	}

	// 2-4. deterministic ordering: priority desc, cost asc, id asc
	sort.SliceStable(applicable, func(i, j int) bool {
		a, b := applicable[i], applicable[j]
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if math.Abs(a.Cost-b.Cost) >= CostEpsilon {
			return a.Cost < b.Cost
		}
		return a.Behavior.ID() < b.Behavior.ID()
	})

	winner := applicable[0]
	d.WinnerID = winner.Behavior.ID()

	// record every conflict with the rule that resolved it
	for _, c := range applicable[1:] {
		d.Conflicts = append(d.Conflicts, Conflict{
			WinnerID: d.WinnerID,
			LoserID:  c.Behavior.ID(),
			Rule:     conflictRule(winner, c),
		})
	}

	// 5. preemption
	if activeID != "" && activeID != d.WinnerID {
		d.PreemptedFrom = activeID
		d.Reason = fmt.Sprintf("preempted %q by %q (priority=%d cost=%.3f)",
			activeID, d.WinnerID, winner.Priority, winner.Cost)
	} else {
		d.Reason = fmt.Sprintf("selected %q (priority=%d cost=%.3f, %d candidate(s))",
			d.WinnerID, winner.Priority, winner.Cost, len(applicable))
	}

	// execute the winner; an error aborts the whole frame
	writes, err := winner.Behavior.Execute(ctx)
	if err != nil {
		d.Err = err
		return d
	}
	d.Writes = writes
	return d
}

// conflictRule explains why winner beat loser.
func conflictRule(winner, loser Candidate) string {
	switch {
	case winner.Priority > loser.Priority:
		return "higher priority"
	case math.Abs(winner.Cost-loser.Cost) >= CostEpsilon && winner.Cost < loser.Cost:
		return "lower cost"
	default:
		return "lexicographic tie-break"
	}
}

// sortedBehaviors returns behaviors sorted by ID (deterministic evaluation).
func sortedBehaviors(bs []Behavior) []Behavior {
	out := make([]Behavior, len(bs))
	copy(out, bs)
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}
