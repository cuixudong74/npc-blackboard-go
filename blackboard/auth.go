package blackboard

import "errors"

// Permission bits for grants.
type Permission uint8

const (
	// PermRead allows reading facts in granted namespaces.
	PermRead Permission = 1 << iota
	// PermWrite allows writing facts in granted namespaces.
	PermWrite
)

// Common authorization errors.
var (
	// ErrUnauthorized is returned when an NPC accesses a namespace without
	// the required permission.
	ErrUnauthorized = errors.New("blackboard: unauthorized access to namespace")
	// ErrNoGrant is returned when an NPC has no grants at all.
	ErrNoGrant = errors.New("blackboard: no grants configured")
)

// Grant describes a permission over a namespace.
// If Recursive is true the grant applies to the namespace and all descendants.
type Grant struct {
	Namespace Namespace
	Perm      Permission
	Recursive bool
}

// GrantSet is the set of grants held by one NPC.
type GrantSet struct {
	grants []Grant
}

// NewGrantSet builds a grant set from the given grants.
func NewGrantSet(grants ...Grant) *GrantSet {
	g := &GrantSet{}
	for _, gr := range grants {
		g.Add(gr)
	}
	return g
}

// Add appends a grant.
func (g *GrantSet) Add(grant Grant) {
	g.grants = append(g.grants, grant)
}

// Grants returns a copy of the grants (sorted for determinism).
func (g *GrantSet) Grants() []Grant {
	out := make([]Grant, len(g.grants))
	copy(out, g.grants)
	return out
}

// CanRead reports whether the set grants read access to ns.
func (g *GrantSet) CanRead(ns Namespace) bool {
	return g.check(ns, PermRead)
}

// CanWrite reports whether the set grants write access to ns.
func (g *GrantSet) CanWrite(ns Namespace) bool {
	return g.check(ns, PermWrite)
}

// check is the shared permission check.
//
// A grant matches when:
//   - its namespace equals ns, or
//   - Recursive is true and its namespace is an ancestor of ns.
//
// Root grants (namespace "/") with Recursive match everything.
// If multiple grants match, permission is the union (any match allows).
func (g *GrantSet) check(ns Namespace, perm Permission) bool {
	for _, gr := range g.grants {
		if gr.Perm&perm == 0 {
			continue
		}
		if gr.Namespace == ns {
			return true
		}
		if gr.Recursive && gr.Namespace.IsAncestorOf(ns) {
			return true
		}
	}
	return false
}
