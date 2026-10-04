// Package runtime 提供多 NPC 共享黑板的确定性协同决策运行时。
package runtime

import (
	"sort"
	"sync"
	"time"

	"github.com/cuixudong74/npc-blackboard-go/arbitration"
	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// Config 是引擎可调参数。
type Config struct {
	// DecisionTimeout 是单个 NPC 决策（评估+执行回调）的耗时上界。
	// 超时即取消该 NPC 本周期的写入（事务回滚），慢 NPC 不阻塞整体：
	// 一个 Step 的耗时上界 ≈ DecisionTimeout + 提交阶段 O(总写入数)。
	DecisionTimeout time.Duration
	// SubscriberBuffer 是引擎为 NPC 创建的订阅通道默认缓冲。
	SubscriberBuffer int
}

// TraceEntry 记录一个 NPC 在一个 tick 内的决策结果，用于复盘与复现校验。
type TraceEntry struct {
	Tick      uint64
	NPCID     string
	Decision  arbitration.Decision
	Committed bool   // 本周期写入是否提交（false = 回滚/超时/无写入）
	Note      string // 失败或取消原因
}

// Engine 是决策运行时：持有黑板、NPC 注册表与仲裁器。
//
// 确定性保证：每个 Step 内——
//  1. 先为全部 NPC 拍摄同一逻辑时钟下的快照；
//  2. 各 NPC 并发评估（只读快照 + 写私有事务缓冲，无共享状态）；
//  3. 按 NPC ID 字典序依次提交事务。
//
// 因此结果与并发调度、遍历顺序无关，固定随机种子与输入序列即可复现。
type Engine struct {
	store   *blackboard.Store
	arbiter *arbitration.Arbiter
	cfg     Config
	seed    int64

	mu      sync.Mutex
	npcs    map[string]*NPC
	pending []func() // 在 tick 边界应用的注册表变更
	tick    uint64
	trace   []TraceEntry
}

// NewEngine 创建引擎；seed 固定后全部运行结果确定。
func NewEngine(store *blackboard.Store, cfg Config, seed int64) *Engine {
	if cfg.DecisionTimeout <= 0 {
		cfg.DecisionTimeout = 50 * time.Millisecond
	}
	if cfg.SubscriberBuffer <= 0 {
		cfg.SubscriberBuffer = 64
	}
	return &Engine{
		store:   store,
		arbiter: arbitration.New(),
		npcs:    make(map[string]*NPC),
		cfg:     cfg,
		seed:    seed,
	}
}

// Store 返回引擎持有的黑板。
func (e *Engine) Store() *blackboard.Store { return e.store }

// Tick 返回当前逻辑时钟。
func (e *Engine) Tick() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.tick
}

// AddNPC 动态注册 NPC，在下一个 tick 边界生效，
// 不打断正在进行的决策，也不会丢失其订阅事件。
func (e *Engine) AddNPC(n *NPC) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending = append(e.pending, func() { e.npcs[n.ID] = n })
}

// RemoveNPC 动态移除 NPC，在下一个 tick 边界生效；
// 同时清理其黑板授权与订阅，避免悬挂状态。
func (e *Engine) RemoveNPC(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending = append(e.pending, func() {
		delete(e.npcs, id)
		e.store.RemoveSubject(id)
	})
}

// AddBehavior 为某 NPC 动态添加行为（下一 tick 生效）。
func (e *Engine) AddBehavior(npcID string, b Behavior) {
	e.mu.Lock()
	n, ok := e.npcs[npcID]
	e.mu.Unlock()
	if ok {
		n.AddBehavior(b)
	}
}

// RemoveBehavior 动态移除某 NPC 的行为。
func (e *Engine) RemoveBehavior(npcID, behaviorID string) {
	e.mu.Lock()
	n, ok := e.npcs[npcID]
	e.mu.Unlock()
	if ok {
		n.RemoveBehavior(behaviorID)
	}
}

// Trace 返回至今的决策轨迹（拷贝），用于复盘与复现校验。
func (e *Engine) Trace() []TraceEntry {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]TraceEntry, len(e.trace))
	copy(out, e.trace)
	return out
}

// evalResult 是单个 NPC 一周期的评估产物。
type evalResult struct {
	npc      *NPC
	decision arbitration.Decision
	tx       *blackboard.Tx
	act      Actuator
	actErr   error
	done     chan struct{}
}

// Step 推进一个决策周期。非可重入。
func (e *Engine) Step() {
	e.mu.Lock()
	for _, f := range e.pending {
		f()
	}
	e.pending = nil
	e.tick++
	tick := e.tick
	npcs := make([]*NPC, 0, len(e.npcs))
	for _, n := range e.npcs {
		npcs = append(npcs, n)
	}
	e.mu.Unlock()

	sort.Slice(npcs, func(i, j int) bool { return npcs[i].ID < npcs[j].ID })
	e.store.Advance(tick)

	// 第一阶段：为全部 NPC 拍摄同一时钟下的自洽快照。
	snaps := make(map[string]*blackboard.Snapshot, len(npcs))
	for _, n := range npcs {
		snaps[n.ID] = e.store.Snapshot(n.ID)
	}

	// 第二阶段：并发评估。每个 goroutine 只读自己的快照、
	// 只写自己的事务缓冲，彼此无共享状态，故无数据竞争。
	results := make([]*evalResult, len(npcs))
	for i, n := range npcs {
		r := &evalResult{npc: n, done: make(chan struct{})}
		results[i] = r
		go func() {
			defer close(r.done)
			snap := snaps[n.ID]
			n.mu.RLock()
			current := n.current
			n.mu.RUnlock()
			r.decision = e.arbiter.Arbitrate(snap, current, n.candidates())
			if r.decision.Winner == "" {
				return
			}
			act, ok := n.actuatorOf(r.decision.Winner)
			if !ok || act == nil {
				return
			}
			r.act = act
			r.tx = e.store.Begin(n.ID)
			r.actErr = act(r.tx, snap)
		}()
	}

	// 第三阶段：按 NPC ID 序等待（带超时）并提交。
	// 超时的 NPC 事务被丢弃（全部回滚），不产生部分写入，
	// 其当前行为保持不变，不产生悬挂的抢占状态。
	for _, r := range results {
		entry := TraceEntry{Tick: tick, NPCID: r.npc.ID}
		select {
		case <-r.done:
		case <-time.After(e.cfg.DecisionTimeout):
			// 超时后不再读取 r 的任何字段（goroutine 可能仍在写），
			// 其事务对象被直接丢弃，永远不会提交。
			entry.Committed = false
			entry.Note = "decision timeout; transaction discarded"
			e.appendTrace(entry)
			continue
		}
		entry.Decision = r.decision
		switch {
		case r.decision.Winner == "":
			entry.Note = "no applicable behavior"
		case r.act == nil:
			entry.Note = "winner has no actuator"
			r.npc.setCurrent(r.decision.Winner)
		case r.actErr != nil:
			r.tx.Rollback()
			entry.Note = "actuator failed: " + r.actErr.Error()
		default:
			if err := r.tx.Commit(); err != nil {
				entry.Note = "commit rejected: " + err.Error()
			} else {
				entry.Committed = true
				r.npc.setCurrent(r.decision.Winner)
			}
		}
		e.appendTrace(entry)
	}
}

func (e *Engine) appendTrace(en TraceEntry) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.trace = append(e.trace, en)
}
