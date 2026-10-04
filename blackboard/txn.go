package blackboard

import "sort"

// Write is a fully stamped write: the op plus the resolved write/expire
// ticks and the committing writer (NPC ID).
type Write struct {
	Op         WriteOp
	WriteTick  int64
	ExpireTick int64
	Writer     string
}

// ChangeSet is a validated, immutable batch of writes ready to be committed.
// Building a change set never mutates the current snapshot: the runtime either
// commits the whole batch or discards it, so a failed/canceled decision cycle
// can never leave partial writes or dangling preemption state.
type ChangeSet struct {
	base   *Snapshot
	tick   int64
	writes []Write
}

// NewChangeSet builds a change set from stamped writes.
// Writes are sorted by (namespace, key) so application order is deterministic.
func NewChangeSet(base *Snapshot, tick int64, writes []Write) *ChangeSet {
	ws := make([]Write, len(writes))
	copy(ws, writes)
	sort.SliceStable(ws, func(i, j int) bool {
		if ws[i].Op.Namespace != ws[j].Op.Namespace {
			return ws[i].Op.Namespace < ws[j].Op.Namespace
		}
		return ws[i].Op.Key < ws[j].Op.Key
	})
	return &ChangeSet{base: base, tick: tick, writes: ws}
}

// Writes returns the stamped writes in deterministic order.
func (c *ChangeSet) Writes() []Write {
	out := make([]Write, len(c.writes))
	copy(out, c.writes)
	return out
}

// Tick returns the tick this change set commits at.
func (c *ChangeSet) Tick() int64 { return c.tick }

// Apply builds the next snapshot by applying the change set to the base
// snapshot. It is a pure function of the base snapshot and the change set:
// callers get a brand new snapshot; the base is untouched (copy-on-write at
// the namespace-map level).
func (c *ChangeSet) Apply() *Snapshot {
	if c.base == nil {
		c.base = &Snapshot{data: map[Namespace]map[string]Fact{}}
	}
	data := make(map[Namespace]map[string]Fact, len(c.base.data)+len(c.writes))
	for ns, m := range c.base.data {
		nm := make(map[string]Fact, len(m))
		for k, f := range m {
			nm[k] = f
		}
		data[ns] = nm
	}
	for _, w := range c.writes {
		m := data[w.Op.Namespace]
		if m == nil {
			m = map[string]Fact{}
			data[w.Op.Namespace] = m
		}
		m[w.Op.Key] = Fact{
			Namespace:  w.Op.Namespace,
			Key:        w.Op.Key,
			Value:      w.Op.Value,
			WriteTick:  w.WriteTick,
			ExpireTick: w.ExpireTick,
			Writer:     w.Writer,
		}
	}
	return &Snapshot{tick: c.tick, data: data}
}
