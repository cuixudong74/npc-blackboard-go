package blackboard

import (
	"testing"
)

func TestNamespaceHierarchy(t *testing.T) {
	root := RootNamespace
	world := Namespace("/world")
	room := Namespace("/world/room")
	npc := Namespace("/world/room/npc/42")

	if !root.IsAncestorOf(npc) {
		t.Fatal("root should be ancestor of everything")
	}
	if !world.IsAncestorOf(room) {
		t.Fatal("world should be ancestor of room")
	}
	if room.IsAncestorOf(world) {
		t.Fatal("room is not ancestor of world")
	}
	if !room.IsAncestorOf(room) {
		t.Fatal("namespace is ancestor of itself")
	}
	if got := room.Parent(); got != world {
		t.Fatalf("parent of room = %q, want %q", got, world)
	}
	if got := npc.Parent(); got != Namespace("/world/room/npc") {
		t.Fatalf("parent of npc = %q", got)
	}
	if got := world.Join("room", "npc"); got != Namespace("/world/room/npc") {
		t.Fatalf("join = %q", got)
	}
	if got := ParseNamespace(" /world//room/ "); got != Namespace("/world/room") {
		t.Fatalf("parse = %q", got)
	}
}

func TestGrants(t *testing.T) {
	g := NewGrantSet(
		Grant{Namespace: "/npc", Perm: PermRead | PermWrite, Recursive: true},
		Grant{Namespace: "/world", Perm: PermRead, Recursive: true},
		Grant{Namespace: "/secret", Perm: PermRead},
	)

	cases := []struct {
		ns    Namespace
		read  bool
		write bool
	}{
		{"/npc", true, true},
		{"/npc/guard", true, true},
		{"/npc/guard/deep", true, true},
		{"/world", true, false},
		{"/world/room", true, false},
		{"/secret", true, false},
		{"/secret/deep", false, false}, // not recursive
		{"/other", false, false},
	}
	for _, c := range cases {
		if got := g.CanRead(c.ns); got != c.read {
			t.Errorf("CanRead(%q) = %v, want %v", c.ns, got, c.read)
		}
		if got := g.CanWrite(c.ns); got != c.write {
			t.Errorf("CanWrite(%q) = %v, want %v", c.ns, got, c.write)
		}
	}
}

func TestSnapshotIsImmutableAndConsistent(t *testing.T) {
	bb := NewBlackboard()
	base := bb.Snapshot()

	cs := NewChangeSet(base, 1, []Write{
		{Op: WriteOp{Namespace: "/a", Key: "x", Value: 1}, WriteTick: 1},
		{Op: WriteOp{Namespace: "/a", Key: "y", Value: 2}, WriteTick: 1},
	})
	next := cs.Apply()
	bb.Commit(next)

	// old snapshot unchanged
	if base.Has("/a", "x") {
		t.Fatal("base snapshot must not be mutated")
	}
	// new snapshot has both facts (self-consistent)
	if !next.Has("/a", "x") || !next.Has("/a", "y") {
		t.Fatal("next snapshot missing facts")
	}
	// current blackboard is the new snapshot
	if got := bb.Snapshot(); got != next {
		t.Fatal("blackboard did not install next snapshot")
	}
}

func TestSnapshotCopyOnWriteSharesUntouched(t *testing.T) {
	base := NewSnapshot(1, map[Namespace]map[string]Fact{
		"/a": {"x": {Namespace: "/a", Key: "x", Value: 1, WriteTick: 1}},
		"/b": {"y": {Namespace: "/b", Key: "y", Value: 2, WriteTick: 1}},
	})
	cs := NewChangeSet(base, 2, []Write{
		{Op: WriteOp{Namespace: "/a", Key: "x", Value: 10}, WriteTick: 2},
	})
	next := cs.Apply()

	// /b untouched and identical
	fb, _ := next.Get("/b", "y")
	if fb.Value != 2 {
		t.Fatal("untouched namespace changed")
	}
	// /a updated
	fa, _ := next.Get("/a", "x")
	if fa.Value != 10 || fa.WriteTick != 2 {
		t.Fatal("updated fact wrong")
	}
	// base unchanged
	fa0, _ := base.Get("/a", "x")
	if fa0.Value != 1 {
		t.Fatal("base mutated")
	}
}

func TestFactExpiry(t *testing.T) {
	snap := NewSnapshot(10, map[Namespace]map[string]Fact{
		"/a": {
			"fresh":   {Namespace: "/a", Key: "fresh", Value: 1, WriteTick: 5, ExpireTick: 0},
			"ttl3":    {Namespace: "/a", Key: "ttl3", Value: 2, WriteTick: 5, ExpireTick: 8},
			"ttl10":   {Namespace: "/a", Key: "ttl10", Value: 3, WriteTick: 5, ExpireTick: 15},
		},
	})
	if !snap.Has("/a", "fresh") {
		t.Fatal("never-expire fact should be present")
	}
	if snap.Has("/a", "ttl3") {
		t.Fatal("ttl3 should be expired at tick 10")
	}
	if !snap.Has("/a", "ttl10") {
		t.Fatal("ttl10 should be present at tick 10")
	}
	// expired facts are invisible to Get
	if _, ok := snap.Get("/a", "ttl3"); ok {
		t.Fatal("expired fact must be invisible")
	}
}

func TestViewAuthorization(t *testing.T) {
	snap := NewSnapshot(1, map[Namespace]map[string]Fact{
		"/world":  {"danger": {Namespace: "/world", Key: "danger", Value: true, WriteTick: 1}},
		"/secret": {"code": {Namespace: "/secret", Key: "code", Value: "x", WriteTick: 1}},
	})
	grants := NewGrantSet(Grant{Namespace: "/world", Perm: PermRead, Recursive: true})
	view := NewView(snap, grants)

	if _, err := view.Get("/world", "danger"); err != nil {
		t.Fatalf("authorized read failed: %v", err)
	}
	if _, err := view.Get("/secret", "code"); err != ErrUnauthorized {
		t.Fatalf("unauthorized read should return ErrUnauthorized, got %v", err)
	}
	if _, err := view.Has("/secret", "code"); err != ErrUnauthorized {
		t.Fatalf("unauthorized Has should return ErrUnauthorized, got %v", err)
	}
}

func TestChangeSetDeterministicOrder(t *testing.T) {
	base := NewSnapshot(1, nil)
	writes := []Write{
		{Op: WriteOp{Namespace: "/b", Key: "z"}, WriteTick: 2},
		{Op: WriteOp{Namespace: "/a", Key: "m"}, WriteTick: 2},
		{Op: WriteOp{Namespace: "/a", Key: "a"}, WriteTick: 2},
	}
	cs := NewChangeSet(base, 2, writes)
	got := cs.Writes()
	want := []string{"/a/a", "/a/m", "/b/z"}
	for i, w := range got {
		if string(w.Op.Namespace)+"/"+w.Op.Key != want[i] {
			t.Fatalf("order[%d] = %s/%s, want %s", i, w.Op.Namespace, w.Op.Key, want[i])
		}
	}
}
