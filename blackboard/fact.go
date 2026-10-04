package blackboard

// Fact is a single versioned piece of blackboard state.
//
// Facts carry their write time (WriteTick) and an optional expiry tick
// (ExpireTick). A fact with ExpireTick == 0 never expires. Expired facts are
// invisible to reads but remain physically present until compaction.
type Fact struct {
	Namespace  Namespace
	Key        string
	Value      any
	WriteTick  int64
	ExpireTick int64
	Writer     string
}

// Ident returns the fully qualified key: namespace + "/" + key.
func (f Fact) Ident() string {
	return string(f.Namespace) + "/" + f.Key
}

// IsExpired reports whether the fact is expired at the given tick.
func (f Fact) IsExpired(atTick int64) bool {
	return f.ExpireTick > 0 && f.ExpireTick <= atTick
}

// WriteOp is a request to write one fact during a decision cycle.
//
// TTL is the validity duration in ticks; 0 means "use the namespace/default
// TTL". SourceTick optionally pins the fact's write tick to an earlier tick
// (e.g. an event that arrived late); 0 means "stamp with the current frame
// tick". Late/out-of-order handling is governed by the runtime LatePolicy.
type WriteOp struct {
	Namespace  Namespace
	Key        string
	Value      any
	TTL        int64
	SourceTick int64
}
