package blackboard

import (
	"errors"
	"sort"
	"sync"
)

// ErrUnauthorized 表示越权访问被拒绝。
var ErrUnauthorized = errors.New("blackboard: unauthorized namespace access")

// Store 是黑板的核心存储。所有读取通过 Snapshot 获得自洽视图；
// 所有写入通过事务（Tx）提交，保证一个决策周期内要么全部生效要么全部回退。
type Store struct {
	mu      sync.RWMutex
	data    map[string]map[string]Fact // ns -> key -> fact
	tick    uint64                     // 当前逻辑时钟
	seq     uint64                     // 全局单调序号
	acls    map[string]ACL             // subject -> 授权
	subs    map[uint64]*subscription   // 订阅表
	nextSub uint64
}

// NewStore 创建空黑板。
func NewStore() *Store {
	return &Store{
		data: make(map[string]map[string]Fact),
		acls: make(map[string]ACL),
		subs: make(map[uint64]*subscription),
	}
}

// SetACL 设置主体的读写授权（覆盖式）。
func (s *Store) SetACL(subject string, acl ACL) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acls[subject] = acl
}

// RemoveSubject 移除主体的授权与其全部订阅（NPC 动态删除时调用）。
func (s *Store) RemoveSubject(subject string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.acls, subject)
	for id, sub := range s.subs {
		if sub.owner == subject {
			close(sub.ch)
			delete(s.subs, id)
		}
	}
}

// Tick 返回当前逻辑时钟。
func (s *Store) Tick() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tick
}

// Advance 将逻辑时钟推进到 t（只允许前进，保证时间语义确定）。
func (s *Store) Advance(t uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t > s.tick {
		s.tick = t
	}
}

// Snapshot 返回主体 reader 当前可见的自洽完整快照。
// 快照是数据的独立拷贝，之后对 Store 的写入不影响该快照。
func (s *Store) Snapshot(reader string) *Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	acl := s.acls[reader]
	out := &Snapshot{
		reader: reader,
		tick:   s.tick,
		facts:  make(map[string]map[string]Fact),
	}
	for ns, kv := range s.data {
		if !acl.CanRead(ns) {
			continue
		}
		m := make(map[string]Fact, len(kv))
		for k, f := range kv {
			m[k] = f
		}
		out.facts[ns] = m
	}
	return out
}

// Begin 为写入者开启一个事务。事务内的写入在 Commit 前对外不可见。
func (s *Store) Begin(writer string) *Tx {
	return &Tx{
		store:   s,
		writer:  writer,
		writes:  make(map[string]map[string]any),
		ttls:    make(map[string]map[string]uint64),
		deletes: make(map[string]map[string]bool),
	}
}

// namespaceNames 返回当前全部命名空间（排序后，保证遍历确定）。
func (s *Store) namespaceNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.data))
	for ns := range s.data {
		names = append(names, ns)
	}
	sort.Strings(names)
	return names
}
