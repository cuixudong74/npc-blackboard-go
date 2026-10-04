package runtime

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cuixudong74/npc-blackboard-go/behavior"
	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// Runtime-owned namespace for per-NPC active behavior state.
const runtimeNamespace blackboard.Namespace = "/runtime"

func activeKey(npcID string) string { return "active:" + npcID }

// Frame errors.
var (
	// ErrQueueFull is returned when the event queue is at capacity.
	ErrQueueFull = errors.New("runtime: event queue full")
	// ErrAlreadyRunning is returned when Run is called twice.
	ErrAlreadyRunning = errors.New("runtime: already running")
	// ErrUnauthorizedWrite is returned when an NPC writes outside its grants.
	ErrUnauthorizedWrite = errors.New("runtime: unauthorized write to namespace")
	// ErrFutureTick is returned when a write's SourceTick is in the future.
	ErrFutureTick = errors.New("runtime: source tick is in the future")
	// ErrNoStore is returned when persistence is required but not configured.
	ErrNoStore = errors.New("runtime: no persistence configured")
)

// FrameReport is the deterministic result of one decision cycle.
type FrameReport struct {
	Tick        int64
	Decisions   []behavior.Decision
	Writes      []blackboard.Write
	Dropped     []DroppedWrite
	Conflicts   []WriteConflict
	Preemptions []Preemption
	Err         error
	Duration    time.Duration
}

// DroppedWrite records a write that was not committed (late drop/quarantine).
type DroppedWrite struct {
	NPCID  string
	Op     blackboard.WriteOp
	Reason string
}

// WriteConflict records two NPCs writing the same key in one frame.
type WriteConflict struct {
	Namespace blackboard.Namespace
	Key       string
	Winner    string
	Losers    []string
	Rule      string
}

// Preemption records a behavior being replaced this frame.
type Preemption struct {
	NPCID  string
	FromID string
	ToID   string
	Reason string
}

// npcResult is one NPC's arbitration outcome for a frame.
type npcResult struct {
	pos int
	npc *NPC
	dec behavior.Decision
}

// Step runs one decision cycle synchronously.
//
// The cycle is all-or-nothing: either every NPC's writes (plus runtime state)
// commit atomically, or nothing does. A timeout or a behavior error rolls the
// whole cycle back and leaves the blackboard at the previous tick.
func (r *Runtime) Step() (FrameReport, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.applyPending()

	tick := r.tick + 1
	base := r.bb.Snapshot()
	events := r.queue.Due(tick)
	npcs := r.reg.snapshot()

	report := FrameReport{Tick: tick, Decisions: make([]behavior.Decision, 0, len(npcs))}
	start := time.Now()

	// bounded decision context
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.FrameTimeout)
	defer cancel()

	// --- concurrent evaluation: NPCs read the same immutable snapshot ---
	type job struct {
		pos int
		npc *NPC
	}
	jobs := make(chan job)
	results := make(chan npcResult, len(npcs))
	var wg sync.WaitGroup
	for w := 0; w < r.cfg.WorkerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if ctx.Err() != nil {
					results <- npcResult{pos: j.pos, npc: j.npc, dec: behavior.Decision{
						NPCID: j.npc.ID, Frame: tick, Err: ctx.Err(),
					}}
					continue
				}
				dec := r.evaluateNPC(ctx, tick, base, j.npc, events)
				results <- npcResult{pos: j.pos, npc: j.npc, dec: dec}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	for i, npc := range npcs {
		jobs <- job{pos: i, npc: npc}
	}
	close(jobs)

	// collect with timeout: a slow frame must not block forever
	collected := make([]npcResult, 0, len(npcs))
	timedOut := false
	for len(collected) < len(npcs) {
		select {
		case res, ok := <-results:
			if !ok {
				goto doneCollect
			}
			collected = append(collected, res)
		case <-ctx.Done():
			timedOut = true
			// drain whatever is immediately available without blocking
			for {
				select {
				case res, ok := <-results:
					if !ok {
						goto doneCollect
					}
					collected = append(collected, res)
				default:
					goto doneCollect
				}
			}
		}
	}
doneCollect:
	cancel()
	report.Duration = time.Since(start)

	if timedOut {
		// rollback: nothing committed, events stay queued
		report.Err = ctx.Err()
		report.Decisions = decisionsInOrder(collected)
		r.dispatch(report)
		return report, report.Err
	}

	// sort results by NPC position (lexicographic NPC ID) for deterministic merge
	sort.Slice(collected, func(i, j int) bool { return collected[i].pos < collected[j].pos })
	decisions := make([]behavior.Decision, len(collected))
	for i, c := range collected {
		decisions[i] = c.dec
	}
	report.Decisions = decisions

	// any behavior error aborts the cycle (all-or-nothing)
	for _, dec := range decisions {
		if dec.Err != nil {
			report.Err = dec.Err
			r.dispatch(report)
			return report, report.Err
		}
	}

	// --- merge + validate writes into a single change set ---
	merged, dropped, conflicts, err := r.mergeWrites(tick, base, collected)
	if err != nil {
		report.Err = err
		r.dispatch(report)
		return report, report.Err
	}
	report.Dropped = dropped
	report.Conflicts = conflicts

	// runtime-owned active-behavior writes (part of the same transaction, so
	// rollback restores preemption state automatically)
	activeWrites, preemptions := r.activeWrites(tick, base, collected)
	report.Preemptions = preemptions

	allWrites := make([]blackboard.Write, 0, len(merged)+len(activeWrites))
	allWrites = append(allWrites, merged...)
	allWrites = append(allWrites, activeWrites...)

	cs := blackboard.NewChangeSet(base, tick, allWrites)
	next := cs.Apply()

	// --- commit: atomic pointer swap ---
	r.bb.Commit(next)
	r.tick = tick
	r.queue.Consume(tick)

	report.Writes = cs.Writes()

	// --- persistence (WAL + periodic checkpoint) ---
	if r.store != nil {
		if perr := r.store.AppendFrame(tick, events, cs.Writes()); perr != nil {
			// committed in memory; surface persistence error
			report.Err = perr
		}
		if tick%int64(r.cfg.CheckpointInterval) == 0 {
			if cerr := r.store.Checkpoint(tick, next, r.queue.PendingEvents()); cerr != nil && report.Err == nil {
				report.Err = cerr
			}
		}
	}

	r.dispatch(report)
	return report, report.Err
}

// evaluateNPC runs arbitration for one NPC against the immutable base snapshot.
func (r *Runtime) evaluateNPC(ctx context.Context, tick int64, base *blackboard.Snapshot, npc *NPC, events []behavior.Event) behavior.Decision {
	activeID := ""
	if f, ok := base.Get(runtimeNamespace, activeKey(npc.ID)); ok {
		if s, ok := f.Value.(string); ok {
			activeID = s
		}
	}
	bctx := &behavior.Context{
		Frame:  tick,
		NPCID:  npc.ID,
		View:   blackboard.NewView(base, npc.Grants),
		Events: events,
		RNG:    behavior.NewFrameRNG(r.cfg.Seed, tick, npc.ID),
		Cancel: ctx,
	}
	return behavior.Arbitrate(npc.ID, tick, activeID, npc.Behaviors(), bctx)
}

// mergeWrites validates and stamps every NPC write into one deterministic
// change set. It does not mutate the blackboard.
func (r *Runtime) mergeWrites(tick int64, base *blackboard.Snapshot, results []npcResult) ([]blackboard.Write, []DroppedWrite, []WriteConflict, error) {
	type key struct {
		ns  blackboard.Namespace
		key string
	}
	committed := map[key]blackboard.Write{}
	var dropped []DroppedWrite
	var conflicts []WriteConflict

	for _, res := range results {
		npc := res.npc
		for _, op := range res.dec.Writes {
			// authorization: NPC may only write granted namespaces
			if npc.Grants == nil || !npc.Grants.CanWrite(op.Namespace) {
				return nil, nil, nil, ErrUnauthorizedWrite
			}
			writeTick := tick
			if op.SourceTick > 0 {
				if op.SourceTick > tick {
					return nil, nil, nil, ErrFutureTick
				}
				writeTick = op.SourceTick
			}
			expireTick := int64(0)
			if op.TTL > 0 {
				expireTick = writeTick + op.TTL
			} else if r.cfg.DefaultTTL > 0 {
				expireTick = writeTick + r.cfg.DefaultTTL
			}

			w := blackboard.Write{Op: op, WriteTick: writeTick, ExpireTick: expireTick, Writer: npc.ID}

			// late-write handling against the base snapshot
			if existing, ok := base.Get(op.Namespace, op.Key); ok && existing.WriteTick > writeTick {
				drop, handled := r.handleLate(npc.ID, &w, existing, &dropped)
				if handled {
					if drop {
						continue
					}
					// quarantine rewrote w's namespace/key; fall through
				}
			}

			k := key{w.Op.Namespace, w.Op.Key}
			if prev, ok := committed[k]; ok {
				switch {
				case prev.WriteTick > w.WriteTick:
					// older write arriving after a newer one in the same frame
					d := DroppedWrite{NPCID: npc.ID, Op: w.Op, Reason: "late-write (in-frame)"}
					dropped = append(dropped, d)
					if r.cfg.LatePolicy == LateQuarantine {
						w.Op.Namespace = quarantineNamespace(w.Op.Namespace)
						w.Op.Key = w.Op.Key + "@late"
						committed[key{w.Op.Namespace, w.Op.Key}] = w
					}
					continue
				case prev.WriteTick == w.WriteTick:
					// deterministic tie-break: lexicographically larger writer wins
					winner, loser := prev.Writer, w.Writer
					if w.Writer > prev.Writer {
						committed[k] = w
						winner, loser = w.Writer, prev.Writer
					}
					conflicts = append(conflicts, WriteConflict{
						Namespace: op.Namespace, Key: op.Key,
						Winner: winner, Losers: []string{loser},
						Rule: "equal write tick; lexicographic writer tie-break",
					})
					continue
				default:
					// newer write tick wins
					conflicts = append(conflicts, WriteConflict{
						Namespace: op.Namespace, Key: op.Key,
						Winner: w.Writer, Losers: []string{prev.Writer},
						Rule: "newer write tick wins",
					})
					committed[k] = w
					continue
				}
			}
			committed[k] = w
		}
	}

	writes := make([]blackboard.Write, 0, len(committed))
	for _, w := range committed {
		writes = append(writes, w)
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Op.Namespace != writes[j].Op.Namespace {
			return writes[i].Op.Namespace < writes[j].Op.Namespace
		}
		return writes[i].Op.Key < writes[j].Op.Key
	})
	return writes, dropped, conflicts, nil
}

// handleLate applies the late-write policy. It returns (drop, handled).
// For LateQuarantine it rewrites w's namespace/key in place.
func (r *Runtime) handleLate(npcID string, w *blackboard.Write, existing blackboard.Fact, dropped *[]DroppedWrite) (bool, bool) {
	switch r.cfg.LatePolicy {
	case LateOverwrite:
		return false, false // let it overwrite (recorded via conflict)
	case LateQuarantine:
		*dropped = append(*dropped, DroppedWrite{
			NPCID: npcID, Op: w.Op,
			Reason: "late write quarantined (existing tick " + strconv.FormatInt(existing.WriteTick, 10) + ")",
		})
		w.Op.Namespace = quarantineNamespace(w.Op.Namespace)
		w.Op.Key = w.Op.Key + "@late"
		return false, true
	default: // LateDrop
		*dropped = append(*dropped, DroppedWrite{
			NPCID: npcID, Op: w.Op,
			Reason: "late write dropped (existing tick " + strconv.FormatInt(existing.WriteTick, 10) + ")",
		})
		return true, true
	}
}

// activeWrites produces runtime-owned active-behavior writes and preemption
// records. These writes are part of the same transaction as NPC writes.
func (r *Runtime) activeWrites(tick int64, base *blackboard.Snapshot, results []npcResult) ([]blackboard.Write, []Preemption) {
	var writes []blackboard.Write
	var preemptions []Preemption
	for _, res := range results {
		npc := res.npc
		dec := res.dec
		activeID := ""
		if f, ok := base.Get(runtimeNamespace, activeKey(npc.ID)); ok {
			if s, ok := f.Value.(string); ok {
				activeID = s
			}
		}
		if dec.WinnerID != activeID {
			val := dec.WinnerID
			writes = append(writes, blackboard.Write{
				Op:        blackboard.WriteOp{Namespace: runtimeNamespace, Key: activeKey(npc.ID), Value: val},
				WriteTick: tick,
				Writer:    "runtime",
			})
			preemptions = append(preemptions, Preemption{
				NPCID: npc.ID, FromID: activeID, ToID: dec.WinnerID,
				Reason: dec.Reason,
			})
		}
	}
	return writes, preemptions
}

func decisionsInOrder(results []npcResult) []behavior.Decision {
	sort.Slice(results, func(i, j int) bool { return results[i].pos < results[j].pos })
	out := make([]behavior.Decision, len(results))
	for i, r := range results {
		out[i] = r.dec
	}
	return out
}

func quarantineNamespace(ns blackboard.Namespace) blackboard.Namespace {
	return blackboard.Namespace("/quarantine").Join(string(ns))
}

