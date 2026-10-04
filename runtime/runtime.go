package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cuixudong74/npc-blackboard-go/behavior"
	"github.com/cuixudong74/npc-blackboard-go/blackboard"
	"github.com/cuixudong74/npc-blackboard-go/persist"
)

// Runtime is the NPC blackboard decision runtime.
type Runtime struct {
	cfg   Config
	bb    *blackboard.Blackboard
	queue *EventQueue
	reg   *registry
	store Store

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	tick    int64

	subMu sync.Mutex
	subs  []*Subscriber

	// pending registry mutations applied at the next frame boundary
	pendingAdd    []*NPC
	pendingRemove []string
	pendingBehAdd []behAdd
	pendingBehDel []behDel
}

type behAdd struct {
	npcID string
	b     behavior.Behavior
}

type behDel struct {
	npcID string
	bID   string
}

// Store is the persistence interface (implemented by persist.FileStore).
type Store interface {
	AppendFrame(tick int64, events []Event, writes []blackboard.Write) error
	AppendEnqueue(ev Event) error
	Checkpoint(tick int64, snap *blackboard.Snapshot, pending []Event) error
	Load() (tick int64, snap *blackboard.Snapshot, pending []Event, err error)
	Close() error
}

// New creates a runtime with the given configuration.
// If PersistPath is set, the runtime resumes from the latest committed state.
func New(cfg Config) (*Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	r := &Runtime{
		cfg:   cfg,
		bb:    blackboard.NewBlackboard(),
		queue: NewEventQueue(cfg.QueueCapacity),
		reg:   newRegistry(),
	}
	if cfg.PersistPath != "" {
		store, err := persist.Open(cfg.PersistPath)
		if err != nil {
			return nil, err
		}
		r.store = store
		tick, snap, pending, err := store.Load()
		if err != nil {
			return nil, err
		}
		r.tick = tick
		if snap != nil {
			r.bb.Commit(snap)
		}
		for _, ev := range pending {
			r.queue.Restore(ev)
		}
	}
	return r, nil
}

// Tick returns the last committed frame tick.
func (r *Runtime) Tick() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tick
}

// Blackboard returns the underlying blackboard.
func (r *Runtime) Blackboard() *blackboard.Blackboard { return r.bb }

// EnqueueEvent enqueues an input event.
// DeliverTick == 0 means the event is delivered next frame.
func (r *Runtime) EnqueueEvent(ev Event) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	seq, ok := r.queue.Enqueue(ev, r.tick)
	if !ok {
		return 0, ErrQueueFull
	}
	ev.Seq = seq
	if r.store != nil {
		if err := r.store.AppendEnqueue(ev); err != nil {
			return seq, err
		}
	}
	return seq, nil
}

// AddNPC queues an NPC for addition at the next frame boundary.
func (r *Runtime) AddNPC(npc *NPC) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pendingAdd = append(r.pendingAdd, npc)
}

// RemoveNPC queues an NPC for removal at the next frame boundary.
func (r *Runtime) RemoveNPC(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pendingRemove = append(r.pendingRemove, id)
}

// AddBehavior queues a behavior addition at the next frame boundary.
func (r *Runtime) AddBehavior(npcID string, b behavior.Behavior) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pendingBehAdd = append(r.pendingBehAdd, behAdd{npcID: npcID, b: b})
}

// RemoveBehavior queues a behavior removal at the next frame boundary.
func (r *Runtime) RemoveBehavior(npcID, bID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pendingBehDel = append(r.pendingBehDel, behDel{npcID: npcID, bID: bID})
}

// applyPending applies queued registry mutations at a frame boundary.
// Called with r.mu held.
func (r *Runtime) applyPending() {
	for _, npc := range r.pendingAdd {
		r.reg.add(npc)
	}
	r.pendingAdd = nil
	for _, id := range r.pendingRemove {
		r.reg.remove(id)
	}
	r.pendingRemove = nil
	for _, ba := range r.pendingBehAdd {
		if npc, ok := r.reg.get(ba.npcID); ok {
			npc.AddBehavior(ba.b)
		}
	}
	r.pendingBehAdd = nil
	for _, bd := range r.pendingBehDel {
		if npc, ok := r.reg.get(bd.npcID); ok {
			npc.RemoveBehavior(bd.bID)
		}
	}
	r.pendingBehDel = nil
}

// Subscribe creates a named subscriber with a bounded channel.
// A slow subscriber never blocks the frame loop: reports are dropped and
// counted.
func (r *Runtime) Subscribe(name string) *Subscriber {
	s := &Subscriber{name: name, ch: make(chan FrameReport, r.cfg.SubscribeBuffer)}
	r.subMu.Lock()
	r.subs = append(r.subs, s)
	r.subMu.Unlock()
	return s
}

// dispatch delivers a report to all subscribers without blocking.
func (r *Runtime) dispatch(rep FrameReport) {
	r.subMu.Lock()
	subs := make([]*Subscriber, len(r.subs))
	copy(subs, r.subs)
	r.subMu.Unlock()
	for _, s := range subs {
		s.deliver(rep)
	}
}

// Run starts the frame loop until ctx is canceled.
func (r *Runtime) Run(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return ErrAlreadyRunning
	}
	r.running = true
	loopCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.running = false
		r.cancel = nil
		r.mu.Unlock()
	}()

	var ticker *time.Ticker
	if r.cfg.FrameInterval > 0 {
		ticker = time.NewTicker(r.cfg.FrameInterval)
		defer ticker.Stop()
	}
	for {
		select {
		case <-loopCtx.Done():
			return loopCtx.Err()
		default:
		}
		if _, err := r.Step(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// frame rolled back; continue the loop
			} else {
				return err
			}
		}
		if ticker != nil {
			select {
			case <-loopCtx.Done():
				return loopCtx.Err()
			case <-ticker.C:
			}
		} else {
			// yield to avoid a 100% CPU spin when no frame interval is set
			select {
			case <-loopCtx.Done():
				return loopCtx.Err()
			case <-time.After(time.Microsecond):
			}
		}
	}
}

// Close stops the runtime and flushes persistence.
func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Unlock()

	r.subMu.Lock()
	subs := r.subs
	r.subMu.Unlock()
	for _, s := range subs {
		s.Close()
	}

	if r.store != nil {
		return r.store.Close()
	}
	return nil
}
