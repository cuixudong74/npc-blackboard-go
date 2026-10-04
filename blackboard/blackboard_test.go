package blackboard

import (
	"fmt"
	"sync"
	"testing"
)

func newTestStore() *Store {
	s := NewStore()
	s.SetACL("npc-a", ACL{Read: []Scope{"/"}, Write: []Scope{"shared", "a"}})
	s.SetACL("npc-b", ACL{Read: []Scope{"shared"}, Write: []Scope{"shared", "b"}})
	return s
}

func TestSnapshotIsolationAndACL(t *testing.T) {
	s := newTestStore()
	tx := s.Begin("npc-a")
	if err := tx.Put("shared", "flag", 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("a", "secret", "x", 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// npc-b 只能看到 shared 命名空间。
	snapB := s.Snapshot("npc-b")
	if _, err := snapB.Get("shared", "flag"); err != nil {
		t.Fatalf("npc-b should read shared/flag: %v", err)
	}
	if _, err := snapB.Get("a", "secret"); err != ErrNotFound {
		t.Fatalf("npc-b must not see a/secret, got %v", err)
	}
	if got := snapB.All("a"); len(got) != 0 {
		t.Fatalf("npc-b must not list namespace a")
	}

	// 越权写：整个事务被拒绝，不留部分写入。
	tx = s.Begin("npc-b")
	_ = tx.Put("shared", "ok", 1, 0)
	_ = tx.Put("a", "hack", 1, 0)
	if err := tx.Commit(); err != ErrUnauthorized {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
	if _, err := s.Snapshot("npc-a").Get("shared", "ok"); err != ErrNotFound {
		t.Fatalf("rejected tx must leave no partial writes")
	}
}

func TestSnapshotSelfConsistent(t *testing.T) {
	s := newTestStore()
	tx := s.Begin("npc-a")
	_ = tx.Put("shared", "x", 1, 0)
	_ = tx.Put("shared", "y", 2, 0)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	snap := s.Snapshot("npc-a")
	// 提交另一个事务后，旧快照必须保持不变（自洽完整快照）。
	tx2 := s.Begin("npc-a")
	_ = tx2.Put("shared", "x", 99, 0)
	_ = tx2.Delete("shared", "y")
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	f, err := snap.Get("shared", "x")
	if err != nil || f.Value != 1 {
		t.Fatalf("stale snapshot must be unaffected, got %v %v", f.Value, err)
	}
	if _, err := snap.Get("shared", "y"); err != nil {
		t.Fatalf("deleted fact must remain in old snapshot: %v", err)
	}
}

func TestExpiry(t *testing.T) {
	s := newTestStore()
	tx := s.Begin("npc-a")
	_ = tx.Put("shared", "temp", "v", 3) // 3 tick 有效期
	_ = tx.Put("shared", "perm", "v", 0)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	s.Advance(3)
	if _, err := s.Snapshot("npc-a").Get("shared", "temp"); err != ErrNotFound {
		t.Fatalf("expired fact must not participate in decisions")
	}
	if _, err := s.Snapshot("npc-a").Get("shared", "perm"); err != nil {
		t.Fatalf("permanent fact must survive: %v", err)
	}
}

func TestOutOfOrderDeterministic(t *testing.T) {
	// 乱序更新策略：提交顺序即全序，后到者覆盖先到者。
	s := newTestStore()
	for i, v := range []int{1, 2, 3} {
		tx := s.Begin("npc-a")
		_ = tx.Put("shared", "k", v, 0)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		f, _ := s.Snapshot("npc-a").Get("shared", "k")
		if f.Value != v || f.Seq != uint64(i+1) {
			t.Fatalf("commit %d: got value=%v seq=%d", i, f.Value, f.Seq)
		}
	}
}

func TestRollback(t *testing.T) {
	s := newTestStore()
	tx := s.Begin("npc-a")
	_ = tx.Put("shared", "k", 1, 0)
	tx.Rollback()
	if _, err := s.Snapshot("npc-a").Get("shared", "k"); err != ErrNotFound {
		t.Fatalf("rolled back write must not be visible")
	}
	if err := tx.Commit(); err != ErrTxClosed {
		t.Fatalf("commit after rollback must fail, got %v", err)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	s := newTestStore()
	var wg sync.WaitGroup
	// 多写者并发提交不同键，多读者并发拍快照；
	// go test -race 下验证无数据竞争。
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				tx := s.Begin("npc-a")
				_ = tx.Put("shared", fmt.Sprintf("w%d-k%d", w, i), i, 0)
				if err := tx.Commit(); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				snap := s.Snapshot("npc-a")
				for _, f := range snap.All("shared") {
					_ = f
				}
			}
		}()
	}
	wg.Wait()
	if got := len(s.Snapshot("npc-a").All("shared")); got != 8*50 {
		t.Fatalf("expected 400 committed facts, got %d", got)
	}
}

func TestSubscribeNonBlocking(t *testing.T) {
	s := newTestStore()
	sub := s.Subscribe("npc-b", "shared", 2)
	for i := 0; i < 10; i++ {
		tx := s.Begin("npc-a")
		_ = tx.Put("shared", fmt.Sprintf("k%d", i), i, 0)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	// 慢订阅者不消费：缓冲满后丢弃，但写入方从未阻塞。
	if sub.Dropped() == 0 {
		t.Fatalf("expected dropped events for slow subscriber")
	}
	got := 0
	for range sub.C() {
		got++
		if got == 2 {
			break
		}
	}
	sub.Unsubscribe()
}
