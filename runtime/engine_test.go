package runtime

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cuixudong74/npc-blackboard-go/arbitration"
	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// setupWorld 构建一个两 NPC 共享黑板的场景：
// guard 在发现敌人时从 patrol 切换到 attack；scout 负责上报敌人位置。
func setupWorld() (*Engine, *blackboard.Store) {
	store := blackboard.NewStore()
	for _, id := range []string{"guard", "scout"} {
		store.SetACL(id, blackboard.ACL{
			Read:  []blackboard.Scope{"/"},
			Write: []blackboard.Scope{"shared", blackboard.Scope(id)},
		})
	}
	eng := NewEngine(store, Config{DecisionTimeout: 100 * time.Millisecond}, 42)

	guard := NewNPC("guard")
	guard.AddBehavior(Behavior{
		Candidate: arbitration.StaticBehavior{
			Name: "attack", Prio: 10,
			Condition: func(s *blackboard.Snapshot) bool {
				_, err := s.Get("shared", "enemy")
				return err == nil
			},
		},
		Act: func(tx *blackboard.Tx, s *blackboard.Snapshot) error {
			return tx.Put("guard", "action", "attack", 0)
		},
	})
	guard.AddBehavior(Behavior{
		Candidate: arbitration.StaticBehavior{Name: "patrol", Prio: 1},
		Act: func(tx *blackboard.Tx, s *blackboard.Snapshot) error {
			return tx.Put("guard", "action", "patrol", 0)
		},
	})

	scout := NewNPC("scout")
	scout.AddBehavior(Behavior{
		Candidate: arbitration.StaticBehavior{Name: "report", Prio: 5},
		Act: func(tx *blackboard.Tx, s *blackboard.Snapshot) error {
			// 偶数 tick 报告敌人出现，奇数 tick 报告敌人消失。
			if s.Tick()%2 == 0 {
				return tx.Put("shared", "enemy", "orc", 2)
			}
			return tx.Delete("shared", "enemy")
		},
	})

	eng.AddNPC(guard)
	eng.AddNPC(scout)
	return eng, store
}

func TestBehaviorAdaptsToEnvironment(t *testing.T) {
	eng, store := setupWorld()
	eng.Step() // tick1: scout 删除 enemy（不存在），guard 巡逻
	if got := currentOf(eng, "guard"); got != "patrol" {
		t.Fatalf("tick1 guard=%s", got)
	}
	eng.Step() // tick2: scout 报告敌人（快照在 tick 初拍摄，guard 下 tick 才看到）
	eng.Step() // tick3: guard 看到 enemy → attack
	if got := currentOf(eng, "guard"); got != "attack" {
		t.Fatalf("tick3 guard=%s, want attack", got)
	}
	f, err := store.Snapshot("guard").Get("guard", "action")
	if err != nil || f.Value != "attack" {
		t.Fatalf("blackboard action=%v err=%v", f.Value, err)
	}
}

// currentOf 从决策轨迹中取某 NPC 最后一次获胜行为。
func currentOf(eng *Engine, id string) string {
	var last string
	for _, e := range eng.Trace() {
		if e.NPCID == id {
			last = e.Decision.Winner
		}
	}
	return last
}

func TestDeterministicReplay(t *testing.T) {
	run := func() []TraceEntry {
		eng, _ := setupWorld()
		for i := 0; i < 20; i++ {
			eng.Step()
		}
		return eng.Trace()
	}
	a := run()
	b := run()
	if len(a) != len(b) {
		t.Fatalf("trace length differs: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Decision.Winner != b[i].Decision.Winner ||
			a[i].Committed != b[i].Committed ||
			fmt.Sprint(a[i].Decision.Ranked) != fmt.Sprint(b[i].Decision.Ranked) {
			t.Fatalf("trace diverges at %d: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestCycleRollbackOnActuatorFailure(t *testing.T) {
	store := blackboard.NewStore()
	store.SetACL("npc", blackboard.ACL{Read: []blackboard.Scope{"/"}, Write: []blackboard.Scope{"w"}})
	eng := NewEngine(store, Config{}, 1)

	n := NewNPC("npc")
	fail := errors.New("boom")
	n.AddBehavior(Behavior{
		Candidate: arbitration.StaticBehavior{Name: "risky", Prio: 1},
		Act: func(tx *blackboard.Tx, s *blackboard.Snapshot) error {
			_ = tx.Put("w", "partial", 1, 0) // 这写入必须随失败一起回滚
			return fail
		},
	})
	eng.AddNPC(n)
	eng.Step()

	if _, err := store.Snapshot("npc").Get("w", "partial"); err != blackboard.ErrNotFound {
		t.Fatalf("failed cycle must leave no partial writes")
	}
	tr := eng.Trace()
	if len(tr) != 1 || tr[0].Committed {
		t.Fatalf("trace must record rollback: %+v", tr)
	}
}

func TestCycleRollbackOnTimeout(t *testing.T) {
	store := blackboard.NewStore()
	store.SetACL("slow", blackboard.ACL{Read: []blackboard.Scope{"/"}, Write: []blackboard.Scope{"w"}})
	store.SetACL("fast", blackboard.ACL{Read: []blackboard.Scope{"/"}, Write: []blackboard.Scope{"w"}})
	eng := NewEngine(store, Config{DecisionTimeout: 20 * time.Millisecond}, 1)

	slow := NewNPC("slow")
	slow.AddBehavior(Behavior{
		Candidate: arbitration.StaticBehavior{Name: "hang", Prio: 1},
		Act: func(tx *blackboard.Tx, s *blackboard.Snapshot) error {
			time.Sleep(200 * time.Millisecond) // 超过决策超时
			return tx.Put("w", "slow", 1, 0)
		},
	})
	fast := NewNPC("fast")
	fast.AddBehavior(Behavior{
		Candidate: arbitration.StaticBehavior{Name: "work", Prio: 1},
		Act: func(tx *blackboard.Tx, s *blackboard.Snapshot) error {
			return tx.Put("w", "fast", 1, 0)
		},
	})
	eng.AddNPC(slow)
	eng.AddNPC(fast)

	start := time.Now()
	eng.Step()
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("slow NPC blocked the step: %v", elapsed)
	}
	// 慢 NPC 的写入被丢弃，快 NPC 的写入正常提交。
	if _, err := store.Snapshot("fast").Get("w", "slow"); err != blackboard.ErrNotFound {
		t.Fatalf("timed-out tx must be discarded")
	}
	if _, err := store.Snapshot("fast").Get("w", "fast"); err != nil {
		t.Fatalf("fast NPC must commit: %v", err)
	}
}

func TestDynamicAddRemove(t *testing.T) {
	eng, store := setupWorld()
	eng.Step()

	// 动态加入新 NPC，下一 tick 即参与决策。
	mage := NewNPC("mage")
	store.SetACL("mage", blackboard.ACL{Read: []blackboard.Scope{"/"}, Write: []blackboard.Scope{"mage"}})
	mage.AddBehavior(Behavior{
		Candidate: arbitration.StaticBehavior{Name: "cast", Prio: 3},
		Act: func(tx *blackboard.Tx, s *blackboard.Snapshot) error {
			return tx.Put("mage", "spell", "shield", 0)
		},
	})
	eng.AddNPC(mage)
	eng.Step()
	if got := currentOf(eng, "mage"); got != "cast" {
		t.Fatalf("dynamically added NPC must decide, got %s", got)
	}

	// 动态移除行为：guard 失去 attack，只能 patrol。
	eng.RemoveBehavior("guard", "attack")
	eng.Step()
	if got := currentOf(eng, "guard"); got != "patrol" {
		t.Fatalf("after behavior removal guard=%s", got)
	}

	// 动态移除 NPC：不再出现在轨迹中。
	eng.RemoveNPC("mage")
	eng.Step()
	last := eng.Trace()[len(eng.Trace())-1]
	for _, e := range eng.Trace()[len(eng.Trace())-2:] {
		if e.NPCID == "mage" {
			t.Fatalf("removed NPC must not decide: %+v", e)
		}
	}
	_ = last
}

func TestSaveLoadRestart(t *testing.T) {
	eng, _ := setupWorld()
	for i := 0; i < 6; i++ {
		eng.Step()
	}
	path := t.TempDir() + "/state.json"
	if err := eng.Save(path); err != nil {
		t.Fatal(err)
	}
	traceBefore := eng.Trace()

	// 模拟重启：全新引擎 + 相同行为注册，从检查点恢复。
	eng2, _ := setupWorld()
	if err := eng2.Load(path); err != nil {
		t.Fatal(err)
	}
	if eng2.Tick() != eng.Tick() {
		t.Fatalf("tick not restored: %d vs %d", eng2.Tick(), eng.Tick())
	}
	for i := 0; i < 6; i++ {
		eng.Step()
		eng2.Step()
	}
	// 重启后继续推进，决策序列必须与未重启完全一致。
	a := eng.Trace()[len(traceBefore):]
	b := eng2.Trace()
	if len(a) != len(b) {
		t.Fatalf("post-restart trace length differs: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Decision.Winner != b[i].Decision.Winner || a[i].Committed != b[i].Committed {
			t.Fatalf("post-restart divergence at %d: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestHighConcurrencyNoStarvation(t *testing.T) {
	store := blackboard.NewStore()
	eng := NewEngine(store, Config{DecisionTimeout: 50 * time.Millisecond}, 7)
	const n = 32
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("npc-%02d", i)
		store.SetACL(id, blackboard.ACL{Read: []blackboard.Scope{"/"}, Write: []blackboard.Scope{blackboard.Scope(id)}})
		npc := NewNPC(id)
		npc.AddBehavior(Behavior{
			Candidate: arbitration.StaticBehavior{Name: "work", Prio: 1},
			Act: func(tx *blackboard.Tx, s *blackboard.Snapshot) error {
				return tx.Put(id, "done", s.Tick(), 0)
			},
		})
		eng.AddNPC(npc)
	}
	for i := 0; i < 10; i++ {
		eng.Step()
	}
	// 每个 NPC 每个 tick 都有已提交的决策：无饥饿。
	committed := make(map[string]int)
	for _, e := range eng.Trace() {
		if e.Committed {
			committed[e.NPCID]++
		}
	}
	if len(committed) != n {
		t.Fatalf("expected %d NPCs committed, got %d", n, len(committed))
	}
	for id, c := range committed {
		if c != 10 {
			t.Fatalf("%s committed %d/10 ticks (starvation)", id, c)
		}
	}
}
