package runtime

import (
	"sort"

	"github.com/cuixudong74/npc-blackboard-go/behavior"
	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// NPC is a non-player character with its own grants and behavior set.
type NPC struct {
	ID        string
	Grants    *blackboard.GrantSet
	behaviors []behavior.Behavior
}

// NewNPC creates an NPC.
func NewNPC(id string, grants *blackboard.GrantSet, behaviors ...behavior.Behavior) *NPC {
	return &NPC{ID: id, Grants: grants, behaviors: behaviors}
}

// Behaviors returns the registered behaviors.
func (n *NPC) Behaviors() []behavior.Behavior { return n.behaviors }

// AddBehavior appends a behavior.
func (n *NPC) AddBehavior(b behavior.Behavior) {
	n.behaviors = append(n.behaviors, b)
}

// RemoveBehavior removes the behavior with the given ID.
func (n *NPC) RemoveBehavior(id string) bool {
	for i, b := range n.behaviors {
		if b.ID() == id {
			n.behaviors = append(n.behaviors[:i], n.behaviors[i+1:]...)
			return true
		}
	}
	return false
}

// registry is the NPC registry with sorted iteration.
type registry struct {
	npcs map[string]*NPC
}

func newRegistry() *registry {
	return &registry{npcs: map[string]*NPC{}}
}

func (r *registry) add(npc *NPC) {
	r.npcs[npc.ID] = npc
}

func (r *registry) remove(id string) {
	delete(r.npcs, id)
}

func (r *registry) get(id string) (*NPC, bool) {
	n, ok := r.npcs[id]
	return n, ok
}

// sortedIDs returns NPC IDs in lexicographic order (deterministic).
func (r *registry) sortedIDs() []string {
	out := make([]string, 0, len(r.npcs))
	for id := range r.npcs {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (r *registry) snapshot() []*NPC {
	ids := r.sortedIDs()
	out := make([]*NPC, 0, len(ids))
	for _, id := range ids {
		out = append(out, r.npcs[id])
	}
	return out
}
