package persist_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/cuixudong74/npc-blackboard-go/behavior"
	"github.com/cuixudong74/npc-blackboard-go/blackboard"
	rt "github.com/cuixudong74/npc-blackboard-go/runtime"
)

// writeBehavior writes a value to its own namespace.
type writeBehavior struct {
	id       string
	priority int
	value    any
}

func (b writeBehavior) ID() string                      { return b.id }
func (b writeBehavior) Priority() int                   { return b.priority }
func (b writeBehavior) Applicable(ctx *behavior.Context) bool { return true }
func (b writeBehavior) Cost(ctx *behavior.Context) float64    { return 1 }
func (b writeBehavior) Execute(ctx *behavior.Context) ([]blackboard.WriteOp, error) {
	return []blackboard.WriteOp{{
		Namespace: blackboard.Namespace("/npc/" + ctx.NPCID),
		Key:       "state",
		Value:     b.value,
	}}, nil
}

func grants() *blackboard.GrantSet {
	return blackboard.NewGrantSet(
		blackboard.Grant{Namespace: "/npc", Perm: blackboard.PermRead | blackboard.PermWrite, Recursive: true},
		blackboard.Grant{Namespace: "/world", Perm: blackboard.PermRead, Recursive: true},
	)
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

func TestRestartResumesFromCommittedState(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("npc-restart-%d", os.Getpid()))
	defer os.RemoveAll(dir)

	cfg := rt.DefaultConfig()
	cfg.Seed = 99
	cfg.PersistPath = dir
	cfg.CheckpointInterval = 5

	// run 10 frames
	r, err := rt.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("npc%02d", i)
		r.AddNPC(rt.NewNPC(id, grants(), writeBehavior{id: "w", priority: 1, value: i}))
	}
	for frame := 0; frame < 10; frame++ {
		if _, err := r.Step(); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen and verify state
	r2, err := rt.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.Tick() != 10 {
		t.Fatalf("tick after restart = %d, want 10", r2.Tick())
	}
	snap := r2.Blackboard().Snapshot()
	if !snap.Has("/npc/npc00", "state") {
		t.Fatal("restored snapshot missing facts")
	}
	f, _ := snap.Get("/npc/npc00", "state")
	if f.Value != 0 {
		t.Fatalf("restored fact value = %v, want 0", f.Value)
	}
}

func TestRestartMatchesContinuousRun(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("npc-continuous-%d", os.Getpid()))
	defer os.RemoveAll(dir)

	makeRuntime := func(persist string) *rt.Runtime {
		cfg := rt.DefaultConfig()
		cfg.Seed = 55
		cfg.PersistPath = persist
		cfg.CheckpointInterval = 3
		r, err := rt.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 4; i++ {
			id := fmt.Sprintf("npc%02d", i)
			r.AddNPC(rt.NewNPC(id, grants(), writeBehavior{id: "w", priority: 1, value: i * 10}))
		}
		return r
	}

	// continuous run: 12 frames
	rc := makeRuntime("")
	var continuous []string
	for frame := 0; frame < 12; frame++ {
		if _, err := rc.Step(); err != nil {
			t.Fatal(err)
		}
		continuous = append(continuous, snapshotFingerprint(rc.Blackboard().Snapshot()))
	}
	rc.Close()

	// interrupted run: 6 frames, restart, 6 more
	ri := makeRuntime(dir)
	for frame := 0; frame < 6; frame++ {
		if _, err := ri.Step(); err != nil {
			t.Fatal(err)
		}
	}
	ri.Close()

	ri2 := makeRuntime(dir)
	if ri2.Tick() != 6 {
		t.Fatalf("tick = %d, want 6", ri2.Tick())
	}
	var interrupted []string
	for frame := 0; frame < 6; frame++ {
		if _, err := ri2.Step(); err != nil {
			t.Fatal(err)
		}
		interrupted = append(interrupted, snapshotFingerprint(ri2.Blackboard().Snapshot()))
	}
	ri2.Close()

	// frames 7-12 must match between continuous and interrupted-then-restarted
	for i := 0; i < 6; i++ {
		if interrupted[i] != continuous[i+6] {
			t.Fatalf("frame %d mismatch after restart:\n continuous: %s\n restarted:  %s",
				i+7, continuous[i+6], interrupted[i])
		}
	}
}

func TestCompositeValuePreservation(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("npc-composite-%d", os.Getpid()))
	defer os.RemoveAll(dir)

	cfg := rt.DefaultConfig()
	cfg.Seed = 3
	cfg.PersistPath = dir
	r, err := rt.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// write a map and a slice via a one-off behavior
	r.AddNPC(rt.NewNPC("a", grants(), &compositeBehavior{}))
	if _, err := r.Step(); err != nil {
		t.Fatal(err)
	}
	r.Close()

	r2, err := rt.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	snap := r2.Blackboard().Snapshot()
	f, ok := snap.Get("/npc/a", "data")
	if !ok {
		t.Fatal("missing composite fact")
	}
	m, ok := f.Value.(map[string]any)
	if !ok {
		t.Fatalf("map became %T after restart", f.Value)
	}
	if m["count"] != 42 {
		t.Fatalf("map count = %v (%T), want int 42", m["count"], m["count"])
	}
	arr, ok := m["items"].([]any)
	if !ok {
		t.Fatalf("items became %T", m["items"])
	}
	if arr[0] != "sword" {
		t.Fatalf("items[0] = %v", arr[0])
	}
}

// compositeBehavior writes a map and a slice.
type compositeBehavior struct{}

func (b *compositeBehavior) ID() string                      { return "composite" }
func (b *compositeBehavior) Priority() int                   { return 1 }
func (b *compositeBehavior) Applicable(ctx *behavior.Context) bool { return true }
func (b *compositeBehavior) Cost(ctx *behavior.Context) float64    { return 1 }
func (b *compositeBehavior) Execute(ctx *behavior.Context) ([]blackboard.WriteOp, error) {
	return []blackboard.WriteOp{{
		Namespace: blackboard.Namespace("/npc/" + ctx.NPCID),
		Key:       "data",
		Value: map[string]any{
			"count": 42,
			"items": []any{"sword", "shield"},
			"ratio": 0.5,
		},
	}}, nil
}

func TestPendingEventsSurviveRestart(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("npc-events-%d", os.Getpid()))
	defer os.RemoveAll(dir)

	cfg := rt.DefaultConfig()
	cfg.Seed = 7
	cfg.PersistPath = dir
	r, err := rt.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.AddNPC(rt.NewNPC("a", grants(), writeBehavior{id: "w", priority: 1, value: 1}))

	// enqueue an event for frame 3
	if _, err := r.EnqueueEvent(rt.Event{Type: "delayed", DeliverTick: 3, Payload: "x"}); err != nil {
		t.Fatal(err)
	}
	// run 2 frames (event not yet delivered)
	for i := 0; i < 2; i++ {
		if _, err := r.Step(); err != nil {
			t.Fatal(err)
		}
	}
	r.Close()

	// reopen and verify state. NPCs are code (behaviors), not data, so they
	// must be re-registered after restart; blackboard state is restored.
	r2, err := rt.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	r2.AddNPC(rt.NewNPC("a", grants(), writeBehavior{id: "w", priority: 1, value: 1}))
	if r2.Tick() != 2 {
		t.Fatalf("tick = %d, want 2", r2.Tick())
	}
	rep, err := r2.Step()
	if err != nil {
		t.Fatal(err)
	}
	if rep.Tick != 3 {
		t.Fatalf("tick = %d, want 3", rep.Tick)
	}
	// the event was delivered this frame (decisions ran)
	if len(rep.Decisions) != 1 {
		t.Fatalf("decisions = %d, want 1", len(rep.Decisions))
	}
}
