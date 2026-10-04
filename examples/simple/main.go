package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cuixudong74/npc-blackboard-go/behavior"
	"github.com/cuixudong74/npc-blackboard-go/blackboard"
	rt "github.com/cuixudong74/npc-blackboard-go/runtime"
)

// --- behaviors ---

type patrol struct{ id string }

func (p patrol) ID() string                                      { return p.id }
func (p patrol) Priority() int                                   { return 1 }
func (p patrol) Applicable(ctx *behavior.Context) bool           { return true }
func (p patrol) Cost(ctx *behavior.Context) float64              { return 1.0 }
func (p patrol) Execute(ctx *behavior.Context) ([]blackboard.WriteOp, error) {
	pos := []string{"A", "B", "C", "D"}
	n := ctx.RNG.Intn(len(pos))
	return []blackboard.WriteOp{{
		Namespace: blackboard.Namespace("/npc/" + ctx.NPCID),
		Key:       "pos",
		Value:     pos[n],
		TTL:       5,
	}}, nil
}

type flee struct{ id string }

func (f flee) ID() string                            { return f.id }
func (f flee) Priority() int                         { return 10 }
func (f flee) Applicable(ctx *behavior.Context) bool {
	for _, ev := range ctx.Events {
		if ev.Type == "danger" {
			b, _ := ev.Payload.(bool)
			return b
		}
	}
	return false
}
func (f flee) Cost(ctx *behavior.Context) float64 { return 0.5 }
func (f flee) Execute(ctx *behavior.Context) ([]blackboard.WriteOp, error) {
	return []blackboard.WriteOp{{
		Namespace: blackboard.Namespace("/npc/" + ctx.NPCID),
		Key:       "pos",
		Value:     "hide",
		TTL:       3,
	}}, nil
}

func main() {
	cfg := rt.DefaultConfig()
	cfg.Seed = 42
	cfg.WorkerCount = 2
	r, err := rt.New(cfg)
	if err != nil {
		panic(err)
	}
	defer r.Close()

	grants := blackboard.NewGrantSet(
		blackboard.Grant{Namespace: "/npc", Perm: blackboard.PermRead | blackboard.PermWrite, Recursive: true},
		blackboard.Grant{Namespace: "/world", Perm: blackboard.PermRead, Recursive: true},
	)
	r.AddNPC(rt.NewNPC("guard", grants, patrol{"guard.patrol"}, flee{"guard.flee"}))
	r.AddNPC(rt.NewNPC("villager", grants, patrol{"villager.patrol"}, flee{"villager.flee"}))

	// frame 1: no danger
	rep, _ := r.Step()
	fmt.Printf("frame %d: ", rep.Tick)
	for _, d := range rep.Decisions {
		fmt.Printf("%s->%s ", d.NPCID, d.WinnerID)
	}
	fmt.Println()

	// frame 2: danger!
	_, _ = r.EnqueueEvent(rt.Event{Type: "danger", DeliverTick: 2, Payload: true})
	rep, _ = r.Step()
	fmt.Printf("frame %d: ", rep.Tick)
	for _, d := range rep.Decisions {
		fmt.Printf("%s->%s(preempt:%v) ", d.NPCID, d.WinnerID, d.PreemptedFrom)
	}
	fmt.Println("writes:", len(rep.Writes))

	// frame 3: danger persists, TTL check
	rep, _ = r.Step()
	fmt.Printf("frame %d: ", rep.Tick)
	for _, d := range rep.Decisions {
		fmt.Printf("%s->%s ", d.NPCID, d.WinnerID)
	}
	fmt.Println()

	// check snapshot
	snap := r.Blackboard().Snapshot()
	for _, ns := range snap.Namespaces() {
		for _, f := range snap.All(ns) {
			fmt.Printf("  %s/%s = %v (tick=%d, expire=%d)\n", ns, f.Key, f.Value, f.WriteTick, f.ExpireTick)
		}
	}

	// unauthorized read attempt
	evil := blackboard.NewView(snap, blackboard.NewGrantSet(
		blackboard.Grant{Namespace: "/secret", Perm: blackboard.PermRead},
	))
	_, err = evil.Get("/world", "danger")
	fmt.Println("unauthorized read err:", err)

	_ = context.Background
	_ = time.Second
}
