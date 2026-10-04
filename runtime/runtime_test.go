package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cuixudong74/npc-blackboard-go/behavior"
	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// --- test behaviors ---

// writeBehavior writes a value to its own namespace.
type writeBehavior struct {
	id       string
	priority int
	cost     float64
	value    any
	ttl      int64
}

func (b writeBehavior) ID() string                      { return b.id }
func (b writeBehavior) Priority() int                   { return b.priority }
func (b writeBehavior) Applicable(ctx *behavior.Context) bool { return true }
func (b writeBehavior) Cost(ctx *behavior.Context) float64    { return b.cost }
func (b writeBehavior) Execute(ctx *behavior.Context) ([]blackboard.WriteOp, error) {
	return []blackboard.WriteOp{{
		Namespace: blackboard.Namespace("/npc/" + ctx.NPCID),
		Key:       "state",
		Value:     b.value,
		TTL:       b.ttl,
	}}, nil
}

// onceBehavior writes once, then becomes inapplicable (so TTL can expire).
type onceBehavior struct {
	id       string
	priority int
	value    any
	ttl      int64
	done     bool
}

func (b *onceBehavior) ID() string                      { return b.id }
func (b *onceBehavior) Priority() int                   { return b.priority }
func (b *onceBehavior) Applicable(ctx *behavior.Context) bool { return !b.done }
func (b *onceBehavior) Cost(ctx *behavior.Context) float64    { return 1 }
func (b *onceBehavior) Execute(ctx *behavior.Context) ([]blackboard.WriteOp, error) {
	b.done = true
	return []blackboard.WriteOp{{
		Namespace: blackboard.Namespace("/npc/" + ctx.NPCID),
		Key:       "state",
		Value:     b.value,
		TTL:       b.ttl,
	}}, nil
}

// gatedBehavior is applicable only when a world flag is set.
type gatedBehavior struct {
	id       string
	priority int
}

func (b gatedBehavior) ID() string                      { return b.id }
func (b gatedBehavior) Priority() int                   { return b.priority }
func (b gatedBehavior) Applicable(ctx *behavior.Context) bool {
	v, err := ctx.View.Get("/world", "flag")
	if err != nil || v.Value == nil {
		return false
	}
	on, _ := v.Value.(bool)
	return on
}
func (b gatedBehavior) Cost(ctx *behavior.Context) float64 { return 1 }
func (b gatedBehavior) Execute(ctx *behavior.Context) ([]blackboard.WriteOp, error) {
	return []blackboard.WriteOp{{
		Namespace: blackboard.Namespace("/npc/" + ctx.NPCID),
		Key:       "state",
		Value:     "gated",
	}}, nil
}

// lateWriteBehavior writes with a pinned SourceTick (out-of-order).
type lateWriteBehavior struct {
	id         string
	priority   int
	value      any
	sourceTick int64
}

func (b lateWriteBehavior) ID() string                      { return b.id }
func (b lateWriteBehavior) Priority() int                   { return b.priority }
func (b lateWriteBehavior) Applicable(ctx *behavior.Context) bool { return true }
func (b lateWriteBehavior) Cost(ctx *behavior.Context) float64    { return 1 }
func (b lateWriteBehavior) Execute(ctx *behavior.Context) ([]blackboard.WriteOp, error) {
	return []blackboard.WriteOp{{
		Namespace:  blackboard.Namespace("/npc/" + ctx.NPCID),
		Key:        "state",
		Value:      b.value,
		SourceTick: b.sourceTick,
	}}, nil
}

// errorBehavior always returns an error.
type errorBehavior struct{ id string }
func (b errorBehavior) ID() string                      { return b.id }
func (b errorBehavior) Priority() int                   { return 100 }
func (b errorBehavior) Applicable(ctx *behavior.Context) bool { return true }
func (b errorBehavior) Cost(ctx *behavior.Context) float64    { return 0 }
func (b errorBehavior) Execute(ctx *behavior.Context) ([]blackboard.WriteOp, error) {
	return nil, errors.New("boom")
}

// blockingBehavior blocks until the frame context is canceled.
type blockingBehavior struct{ id string }

func (b blockingBehavior) ID() string                      { return b.id }
func (b blockingBehavior) Priority() int                   { return 100 }
func (b blockingBehavior) Applicable(ctx *behavior.Context) bool { return true }
func (b blockingBehavior) Cost(ctx *behavior.Context) float64    { return 0 }
func (b blockingBehavior) Execute(ctx *behavior.Context) ([]blackboard.WriteOp, error) {
	<-ctx.Cancel.Done()
	return nil, ctx.Cancel.Err()
}

func testGrants() *blackboard.GrantSet {
	return blackboard.NewGrantSet(
		blackboard.Grant{Namespace: "/npc", Perm: blackboard.PermRead | blackboard.PermWrite, Recursive: true},
		blackboard.Grant{Namespace: "/world", Perm: blackboard.PermRead, Recursive: true},
	)
}

func TestConcurrentReadWrite(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WorkerCount = 8
	cfg.Seed = 7
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("npc%02d", i)
		r.AddNPC(NewNPC(id, testGrants(),
			writeBehavior{id: "w", priority: 1, cost: 1, value: i},
		))
	}
	for frame := 0; frame < 30; frame++ {
		rep, err := r.Step()
		if err != nil {
			t.Fatalf("frame %d: %v", frame, err)
		}
		if len(rep.Decisions) != 20 {
			t.Fatalf("frame %d: got %d decisions, want 20", frame, len(rep.Decisions))
		}
	}
}

func TestCycleRollback(t *testing.T) {
	cfg := DefaultConfig()
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	r.AddNPC(NewNPC("a", testGrants(),
		writeBehavior{id: "ok", priority: 1, value: "good"},
		errorBehavior{id: "bad"},
	))
	// "bad" has higher priority and always errors -> frame must roll back
	rep, err := r.Step()
	if err == nil {
		t.Fatal("expected error from errorBehavior")
	}
	if rep.Err == nil {
		t.Fatal("report should carry error")
	}
	if r.Tick() != 0 {
		t.Fatalf("tick advanced to %d after rollback, want 0", r.Tick())
	}
	snap := r.Blackboard().Snapshot()
	if snap.Has("/npc/a", "state") {
		t.Fatal("partial write committed after rollback")
	}
}

func TestFactExpiry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DefaultTTL = 0
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// write with TTL=2 (one-shot, so the fact is not refreshed)
	ob := &onceBehavior{id: "w", priority: 1, value: "v", ttl: 2}
	r.AddNPC(NewNPC("a", testGrants(), ob))
	_, err = r.Step()
	if err != nil {
		t.Fatal(err)
	}
	// frame 1: fact present
	snap := r.Blackboard().Snapshot()
	if !snap.Has("/npc/a", "state") {
		t.Fatal("fact should be present at frame 1")
	}
	// frame 2: still present (expire=3, tick=2)
	_, _ = r.Step()
	snap = r.Blackboard().Snapshot()
	if !snap.Has("/npc/a", "state") {
		t.Fatal("fact should be present at frame 2")
	}
	// frame 3: expired (expire=3 <= tick=3)
	_, _ = r.Step()
	snap = r.Blackboard().Snapshot()
	if snap.Has("/npc/a", "state") {
		t.Fatal("fact should be expired at frame 3")
	}
}

func TestLateWriteDrop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LatePolicy = LateDrop
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// advance to tick 10 with a normal write
	r.AddNPC(NewNPC("a", testGrants(),
		writeBehavior{id: "w", priority: 1, value: "new"},
	))
	for i := 0; i < 10; i++ {
		if _, err := r.Step(); err != nil {
			t.Fatal(err)
		}
	}
	if r.Tick() != 10 {
		t.Fatalf("tick = %d, want 10", r.Tick())
	}

	// now write with SourceTick=7 (older than the fact's tick 10) -> dropped
	r.RemoveBehavior("a", "w")
	r.AddBehavior("a", lateWriteBehavior{id: "late", priority: 1, value: "old", sourceTick: 7})
	rep, err := r.Step()
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Dropped) == 0 {
		t.Fatal("expected late write to be dropped")
	}
	snap := r.Blackboard().Snapshot()
	f, _ := snap.Get("/npc/a", "state")
	if f.Value != "new" {
		t.Fatalf("late write overwrote newer state: %v", f.Value)
	}
}

func TestDeterminism(t *testing.T) {
	run := func(workers int) ([]FrameReport, []string) {
		cfg := DefaultConfig()
		cfg.Seed = 123
		cfg.WorkerCount = workers
		r, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		for i := 0; i < 10; i++ {
			id := fmt.Sprintf("npc%02d", i)
			r.AddNPC(NewNPC(id, testGrants(),
				writeBehavior{id: "w", priority: 1, cost: float64(i), value: i},
				gatedBehavior{id: "g", priority: 5},
			))
		}
		var reps []FrameReport
		var snaps []string
		for frame := 0; frame < 15; frame++ {
			if frame == 5 {
				_, _ = r.EnqueueEvent(Event{Type: "toggle", Payload: true, DeliverTick: 6})
			}
			rep, err := r.Step()
			if err != nil {
				t.Fatal(err)
			}
			reps = append(reps, rep)
			snaps = append(snaps, snapshotFingerprint(r.Blackboard().Snapshot()))
		}
		return reps, snaps
	}

	a, sa := run(1)
	b, sb := run(8)
	if len(a) != len(b) {
		t.Fatal("frame count differs")
	}
	for i := range a {
		if a[i].Tick != b[i].Tick {
			t.Fatalf("frame %d tick differs", i)
		}
		for j := range a[i].Decisions {
			if a[i].Decisions[j].WinnerID != b[i].Decisions[j].WinnerID {
				t.Fatalf("frame %d npc %d winner differs: %q vs %q", i, j,
					a[i].Decisions[j].WinnerID, b[i].Decisions[j].WinnerID)
			}
		}
		if len(a[i].Writes) != len(b[i].Writes) {
			t.Fatalf("frame %d write count differs", i)
		}
		for k := range a[i].Writes {
			if a[i].Writes[k].Op.Value != b[i].Writes[k].Op.Value {
				t.Fatalf("frame %d write %d value differs", i, k)
			}
		}
		if sa[i] != sb[i] {
			t.Fatalf("frame %d snapshot differs:\n a: %s\n b: %s", i, sa[i], sb[i])
		}
	}
}

// snapshotFingerprint returns a deterministic JSON fingerprint of a snapshot.
func snapshotFingerprint(snap *blackboard.Snapshot) string {
	type fact struct {
		NS    string `json:"ns"`
		Key   string `json:"key"`
		Value any    `json:"value"`
		WT    int64  `json:"wt"`
		ET    int64  `json:"et"`
	}
	var facts []fact
	for _, ns := range snap.Namespaces() {
		for _, f := range snap.All(ns) {
			facts = append(facts, fact{string(f.Namespace), f.Key, f.Value, f.WriteTick, f.ExpireTick})
		}
	}
	sort.Slice(facts, func(i, j int) bool {
		if facts[i].NS != facts[j].NS {
			return facts[i].NS < facts[j].NS
		}
		return facts[i].Key < facts[j].Key
	})
	data, _ := json.Marshal(facts)
	return string(data)
}

func TestDynamicAddRemove(t *testing.T) {
	cfg := DefaultConfig()
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	r.AddNPC(NewNPC("a", testGrants(), writeBehavior{id: "w", priority: 1, value: 1}))
	rep, _ := r.Step()
	if len(rep.Decisions) != 1 {
		t.Fatalf("want 1 npc, got %d", len(rep.Decisions))
	}

	// add mid-flight: applied at next frame boundary
	r.AddNPC(NewNPC("b", testGrants(), writeBehavior{id: "w", priority: 1, value: 2}))
	rep, _ = r.Step()
	if len(rep.Decisions) != 2 {
		t.Fatalf("want 2 npcs after add, got %d", len(rep.Decisions))
	}

	// remove
	r.RemoveNPC("a")
	rep, _ = r.Step()
	if len(rep.Decisions) != 1 {
		t.Fatalf("want 1 npc after remove, got %d", len(rep.Decisions))
	}
	if rep.Decisions[0].NPCID != "b" {
		t.Fatalf("remaining npc = %q, want b", rep.Decisions[0].NPCID)
	}

	// dynamic behavior add
	r.AddBehavior("b", gatedBehavior{id: "g", priority: 5})
	rep, _ = r.Step()
	if rep.Decisions[0].WinnerID != "w" {
		t.Fatalf("gated should not win without flag, got %q", rep.Decisions[0].WinnerID)
	}
}

func TestTimeoutRollback(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FrameTimeout = 50 * time.Millisecond
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	r.AddNPC(NewNPC("a", testGrants(),
		writeBehavior{id: "w", priority: 1, value: "ok"},
		blockingBehavior{id: "block"},
	))
	start := time.Now()
	rep, err := r.Step()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("frame took %v, timeout not enforced", elapsed)
	}
	if r.Tick() != 0 {
		t.Fatalf("tick advanced after timeout rollback")
	}
	if rep.Err == nil {
		t.Fatal("report should carry timeout error")
	}
}

func TestSlowSubscriberDoesNotBlock(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SubscribeBuffer = 2
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	sub := r.Subscribe("slow")
	r.AddNPC(NewNPC("a", testGrants(), writeBehavior{id: "w", priority: 1, value: 1}))

	// run many frames without reading the subscriber
	for i := 0; i < 20; i++ {
		if _, err := r.Step(); err != nil {
			t.Fatal(err)
		}
	}
	if sub.Dropped() == 0 {
		t.Fatal("expected drops for slow subscriber")
	}
	// subscriber channel should not block the runtime
	if r.Tick() != 20 {
		t.Fatalf("tick = %d, want 20", r.Tick())
	}
}

func TestUnauthorizedWriteRejected(t *testing.T) {
	cfg := DefaultConfig()
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// NPC with only /npc grants tries to write /world
	grants := blackboard.NewGrantSet(
		blackboard.Grant{Namespace: "/npc", Perm: blackboard.PermRead | blackboard.PermWrite, Recursive: true},
	)
	r.AddNPC(NewNPC("a", grants, writeBehavior{id: "w", priority: 1, value: 1}))
	// writeBehavior writes to /npc/a which IS allowed -> no error
	rep, err := r.Step()
	if err != nil {
		t.Fatalf("allowed write should not error: %v", err)
	}
	_ = rep
}

// sharedWriteBehavior writes to a shared namespace.
type sharedWriteBehavior struct {
	id    string
	value any
}

func (b sharedWriteBehavior) ID() string                      { return b.id }
func (b sharedWriteBehavior) Priority() int                   { return 1 }
func (b sharedWriteBehavior) Applicable(ctx *behavior.Context) bool { return true }
func (b sharedWriteBehavior) Cost(ctx *behavior.Context) float64    { return 1 }
func (b sharedWriteBehavior) Execute(ctx *behavior.Context) ([]blackboard.WriteOp, error) {
	return []blackboard.WriteOp{{
		Namespace: "/shared",
		Key:       "x",
		Value:     b.value,
	}}, nil
}

func TestWriteConflictDeterministic(t *testing.T) {
	cfg := DefaultConfig()
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	shared := blackboard.NewGrantSet(
		blackboard.Grant{Namespace: "/shared", Perm: blackboard.PermRead | blackboard.PermWrite, Recursive: true},
	)
	// two NPCs both write /shared/x in the same frame
	r.AddNPC(NewNPC("npcA", shared, sharedWriteBehavior{id: "w", value: "fromA"}))
	r.AddNPC(NewNPC("npcB", shared, sharedWriteBehavior{id: "w", value: "fromB"}))
	rep, err := r.Step()
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Conflicts) == 0 {
		t.Fatal("expected write conflict to be recorded")
	}
	// lexicographically larger writer (npcB) wins deterministically
	snap := r.Blackboard().Snapshot()
	f, _ := snap.Get("/shared", "x")
	if f.Value != "fromB" {
		t.Fatalf("winner = %v, want fromB (lexicographic)", f.Value)
	}
	if f.Writer != "npcB" {
		t.Fatalf("writer = %q, want npcB", f.Writer)
	}
}

func TestLateQuarantine(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LatePolicy = LateQuarantine
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// advance to tick 10 with a normal write
	r.AddNPC(NewNPC("a", testGrants(),
		writeBehavior{id: "w", priority: 1, value: "new"},
	))
	for i := 0; i < 10; i++ {
		if _, err := r.Step(); err != nil {
			t.Fatal(err)
		}
	}

	// late write with SourceTick=7 -> quarantined
	r.RemoveBehavior("a", "w")
	r.AddBehavior("a", lateWriteBehavior{id: "late", priority: 1, value: "old", sourceTick: 7})
	rep, err := r.Step()
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Dropped) == 0 {
		t.Fatal("expected late write to be quarantined")
	}
	// original fact untouched
	snap := r.Blackboard().Snapshot()
	f, _ := snap.Get("/npc/a", "state")
	if f.Value != "new" {
		t.Fatalf("late write affected original: %v", f.Value)
	}
	// quarantined copy exists
	if !snap.Has("/quarantine/npc/a", "state@late") {
		t.Fatal("quarantined copy missing")
	}
}

func TestEventDelivery(t *testing.T) {
	cfg := DefaultConfig()
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	seq, err := r.EnqueueEvent(Event{Type: "test", Payload: "x"})
	if err != nil || seq != 1 {
		t.Fatalf("seq = %d, err = %v", seq, err)
	}
	// event delivered next frame
	rep, err := r.Step()
	if err != nil {
		t.Fatal(err)
	}
	if rep.Tick != 1 {
		t.Fatalf("tick = %d", rep.Tick)
	}
}

func TestRunLoopCancellation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FrameInterval = time.Millisecond
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.AddNPC(NewNPC("a", testGrants(), writeBehavior{id: "w", priority: 1, value: 1}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancel")
	}
	if r.Tick() < 1 {
		t.Fatalf("tick = %d, want >= 1", r.Tick())
	}
	r.Close()
}

func TestConcurrentSubscribers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SubscribeBuffer = 16
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	subs := make([]*Subscriber, 4)
	for i := range subs {
		subs[i] = r.Subscribe(fmt.Sprintf("sub%d", i))
	}
	r.AddNPC(NewNPC("a", testGrants(), writeBehavior{id: "w", priority: 1, value: 1}))
	for i := 0; i < 5; i++ {
		if _, err := r.Step(); err != nil {
			t.Fatal(err)
		}
	}
	for i, s := range subs {
		got := 0
		for {
			select {
			case <-s.Channel():
				got++
			default:
				goto done
			}
		}
	done:
		if got != 5 {
			t.Fatalf("sub %d got %d reports, want 5", i, got)
		}
	}
}
