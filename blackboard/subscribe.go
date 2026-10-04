package blackboard

import "sync/atomic"

// Change 描述一次已提交的事实变更。
type Change struct {
	Fact    Fact
	Deleted bool
}

// subscription 是订阅的内部记录。
type subscription struct {
	id      uint64
	owner   string
	prefix  Scope
	ch      chan Change
	dropped atomic.Uint64
}

// Subscription 是订阅句柄。投递是非阻塞的：订阅者消费过慢时
// 丢弃事件并累计 Dropped，慢订阅者永远不会阻塞写入方。
type Subscription struct {
	store *Store
	sub   *subscription
}

// C 返回变更事件通道；退订或主体被移除后通道关闭。
func (s *Subscription) C() <-chan Change { return s.sub.ch }

// Dropped 返回因缓冲满而被丢弃的事件数。
func (s *Subscription) Dropped() uint64 { return s.sub.dropped.Load() }

// Unsubscribe 退订并关闭事件通道。
func (s *Subscription) Unsubscribe() {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if _, ok := s.store.subs[s.sub.id]; ok {
		delete(s.store.subs, s.sub.id)
		close(s.sub.ch)
	}
}

// Subscribe 让 owner 订阅命名空间前缀 prefix 下的全部已提交变更。
// buffer 为通道容量；事件在事务提交时投递，只包含已生效的变更。
func (s *Store) Subscribe(owner string, prefix Scope, buffer int) *Subscription {
	if buffer <= 0 {
		buffer = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSub++
	sub := &subscription{
		id:     s.nextSub,
		owner:  owner,
		prefix: prefix,
		ch:     make(chan Change, buffer),
	}
	s.subs[sub.id] = sub
	return &Subscription{store: s, sub: sub}
}
