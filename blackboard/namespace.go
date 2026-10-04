package blackboard

import (
	"sort"
	"strings"
)

// Namespace is a hierarchical blackboard path, e.g. "/world/room/42".
// Namespaces are slash-separated; "/" is the root namespace.
type Namespace string

// RootNamespace is the top-level namespace.
const RootNamespace Namespace = "/"

// ParseNamespace normalizes a raw path into a Namespace.
// A leading "/" is optional; empty segments are removed.
func ParseNamespace(raw string) Namespace {
	parts := strings.Split(raw, "/")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return RootNamespace
	}
	return Namespace("/" + strings.Join(out, "/"))
}

// Parent returns the parent namespace. The parent of RootNamespace is itself.
func (n Namespace) Parent() Namespace {
	if n == RootNamespace || n == "" {
		return RootNamespace
	}
	s := strings.TrimSuffix(string(n), "/")
	i := strings.LastIndex(s, "/")
	if i <= 0 {
		return RootNamespace
	}
	return Namespace(s[:i])
}

// Join returns a child namespace by appending path segments.
func (n Namespace) Join(segments ...string) Namespace {
	base := strings.TrimSuffix(string(n), "/")
	for _, seg := range segments {
		seg = strings.Trim(strings.TrimSpace(seg), "/")
		if seg == "" {
			continue
		}
		base += "/" + seg
	}
	if base == "" {
		return RootNamespace
	}
	return Namespace(base)
}

// Segments returns the path segments, e.g. ["world","room","42"].
func (n Namespace) Segments() []string {
	s := strings.TrimPrefix(string(n), "/")
	if s == "" {
		return nil
	}
	return strings.Split(s, "/")
}

// IsAncestorOf reports whether n is an ancestor of (or equal to) other.
func (n Namespace) IsAncestorOf(other Namespace) bool {
	if n == other {
		return true
	}
	base := string(n)
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	return strings.HasPrefix(string(other), base)
}

// SortNamespaces returns a sorted copy of ns (deterministic iteration order).
func SortNamespaces(ns []Namespace) []Namespace {
	out := make([]Namespace, len(ns))
	copy(out, ns)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
