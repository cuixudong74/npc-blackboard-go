package blackboard

import "sort"

// Snapshot is an immutable, self-consistent view of the blackboard at one
// committed tick. Once created a snapshot is never mutated: readers always
// observe either the previous complete snapshot or the next complete one,
// never a field-level mix of the two.
type Snapshot struct {
	tick int64
	data map[Namespace]map[string]Fact
}

// NewSnapshot builds a snapshot from raw data. It is intended for persistence
// replay and tests; callers must not mutate data afterwards.
func NewSnapshot(tick int64, data map[Namespace]map[string]Fact) *Snapshot {
	if data == nil {
		data = map[Namespace]map[string]Fact{}
	}
	return &Snapshot{tick: tick, data: data}
}

// Tick returns the commit tick of this snapshot.
func (s *Snapshot) Tick() int64 { return s.tick }

// Get returns the raw fact without authorization checks.
// Expired facts are treated as absent.
func (s *Snapshot) Get(ns Namespace, key string) (Fact, bool) {
	if s == nil || s.data == nil {
		return Fact{}, false
	}
	m := s.data[ns]
	if m == nil {
		return Fact{}, false
	}
	f, ok := m[key]
	if !ok || f.IsExpired(s.tick) {
		return Fact{}, false
	}
	return f, true
}

// Has reports whether a non-expired fact exists at ns/key.
func (s *Snapshot) Has(ns Namespace, key string) bool {
	_, ok := s.Get(ns, key)
	return ok
}

// Namespaces returns the sorted list of namespaces that contain facts.
func (s *Snapshot) Namespaces() []Namespace {
	if s == nil {
		return nil
	}
	out := make([]Namespace, 0, len(s.data))
	for ns := range s.data {
		out = append(out, ns)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Keys returns the sorted keys of non-expired facts in ns.
func (s *Snapshot) Keys(ns Namespace) []string {
	if s == nil || s.data == nil {
		return nil
	}
	m := s.data[ns]
	out := make([]string, 0, len(m))
	for k, f := range m {
		if f.IsExpired(s.tick) {
			continue
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// All returns all non-expired facts in ns, sorted by key.
func (s *Snapshot) All(ns Namespace) []Fact {
	keys := s.Keys(ns)
	out := make([]Fact, 0, len(keys))
	for _, k := range keys {
		f, _ := s.Get(ns, k)
		out = append(out, f)
	}
	return out
}

// View is an authorization-filtered view over a snapshot.
// Every read through a View is checked against the NPC's grants; unauthorized
// reads return ErrUnauthorized (they are never silently dropped).
type View struct {
	snap   *Snapshot
	grants *GrantSet
}

// NewView wraps a snapshot with a grant set.
func NewView(snap *Snapshot, grants *GrantSet) *View {
	return &View{snap: snap, grants: grants}
}

// Get returns the fact at ns/key if the grants allow it.
func (v *View) Get(ns Namespace, key string) (Fact, error) {
	if v == nil || v.snap == nil {
		return Fact{}, ErrUnauthorized
	}
	if v.grants == nil || !v.grants.CanRead(ns) {
		return Fact{}, ErrUnauthorized
	}
	f, ok := v.snap.Get(ns, key)
	if !ok {
		return Fact{}, nil
	}
	return f, nil
}

// Has reports whether a non-expired, authorized fact exists.
func (v *View) Has(ns Namespace, key string) (bool, error) {
	if v == nil || v.snap == nil {
		return false, ErrUnauthorized
	}
	if v.grants == nil || !v.grants.CanRead(ns) {
		return false, ErrUnauthorized
	}
	return v.snap.Has(ns, key), nil
}

// Snapshot returns the underlying (unauthorized) snapshot.
func (v *View) Snapshot() *Snapshot { return v.snap }
