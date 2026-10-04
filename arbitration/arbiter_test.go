package arbitration

import (
	"testing"

	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

func snap() *blackboard.Snapshot {
	s := blackboard.NewStore()
	s.SetACL("sys", blackboard.ACL{Read: []blackboard.Scope{"/"}, Write: []blackboard.Scope{"/"}})
	return s.Snapshot("sys")
}

func TestPriorityWins(t *testing.T) {
	a := New()
	d := a.Arbitrate(snap(), "", []Behavior{
		StaticBehavior{Name: "idle", Prio: 1},
		StaticBehavior{Name: "flee", Prio: 10},
		StaticBehavior{Name: "patrol", Prio: 5},
	})
	if d.Winner != "flee" {
		t.Fatalf("higher priority must win, got %s", d.Winner)
	}
	if d.Ranked[0] != "flee" || d.Ranked[1] != "patrol" || d.Ranked[2] != "idle" {
		t.Fatalf("ranked order wrong: %v", d.Ranked)
	}
	if d.Reason == "" {
		t.Fatalf("decision must be explainable")
	}
}

func TestCostBreaksTie(t *testing.T) {
	a := New()
	d := a.Arbitrate(snap(), "", []Behavior{
		StaticBehavior{Name: "b", Prio: 5, CostFn: func(*blackboard.Snapshot) float64 { return 2 }},
		StaticBehavior{Name: "a", Prio: 5, CostFn: func(*blackboard.Snapshot) float64 { return 1 }},
	})
	if d.Winner != "a" {
		t.Fatalf("lower cost must win tie, got %s", d.Winner)
	}
}

func TestIDBreaksFullTie(t *testing.T) {
	a := New()
	// 与传入顺序无关：调换顺序结果必须一致。
	d1 := a.Arbitrate(snap(), "", []Behavior{
		StaticBehavior{Name: "beta", Prio: 5},
		StaticBehavior{Name: "alpha", Prio: 5},
	})
	d2 := a.Arbitrate(snap(), "", []Behavior{
		StaticBehavior{Name: "alpha", Prio: 5},
		StaticBehavior{Name: "beta", Prio: 5},
	})
	if d1.Winner != "alpha" || d2.Winner != "alpha" {
		t.Fatalf("lexicographically smaller ID must win full tie: %s %s", d1.Winner, d2.Winner)
	}
}

func TestApplicabilityFilter(t *testing.T) {
	a := New()
	d := a.Arbitrate(snap(), "", []Behavior{
		StaticBehavior{Name: "high", Prio: 10, Condition: func(*blackboard.Snapshot) bool { return false }},
		StaticBehavior{Name: "low", Prio: 1},
	})
	if d.Winner != "low" {
		t.Fatalf("inapplicable candidate must be filtered, got %s", d.Winner)
	}
}

func TestPreemption(t *testing.T) {
	a := New()
	cands := []Behavior{
		StaticBehavior{Name: "patrol", Prio: 5},
		StaticBehavior{Name: "flee", Prio: 10},
	}
	// 更高优先级候选出现 → 抢占。
	d := a.Arbitrate(snap(), "patrol", cands)
	if d.Winner != "flee" || d.Preempted != "patrol" {
		t.Fatalf("expected flee preempts patrol, got %+v", d)
	}
	// 当前行为优先级更高 → 不抢占。
	d = a.Arbitrate(snap(), "flee", cands)
	if d.Winner != "flee" || d.Preempted != "" {
		t.Fatalf("lower priority candidate must not preempt, got %+v", d)
	}
	// 同优先级不抢占（避免抖动）。
	d = a.Arbitrate(snap(), "patrol", []Behavior{
		StaticBehavior{Name: "patrol", Prio: 5},
		StaticBehavior{Name: "other", Prio: 5},
	})
	if d.Winner != "patrol" || d.Preempted != "" {
		t.Fatalf("equal priority must not preempt, got %+v", d)
	}
}

func TestCurrentConditionLost(t *testing.T) {
	a := New()
	d := a.Arbitrate(snap(), "patrol", []Behavior{
		StaticBehavior{Name: "patrol", Prio: 5, Condition: func(*blackboard.Snapshot) bool { return false }},
		StaticBehavior{Name: "idle", Prio: 1},
	})
	if d.Winner != "idle" || d.Preempted != "" {
		t.Fatalf("condition-lost current must yield without preemption, got %+v", d)
	}
}

func TestNoApplicable(t *testing.T) {
	a := New()
	d := a.Arbitrate(snap(), "", []Behavior{
		StaticBehavior{Name: "x", Condition: func(*blackboard.Snapshot) bool { return false }},
	})
	if d.Winner != "" {
		t.Fatalf("expected no winner, got %s", d.Winner)
	}
}
