package blackboard

import "sort"

// Tx 是一个决策周期内的写事务：所有写入先缓冲，
// Commit 时原子生效，Rollback 时全部丢弃，绝不留下部分写入。
type Tx struct {
	store   *Store
	writer  string
	writes  map[string]map[string]any    // ns -> key -> value
	ttls    map[string]map[string]uint64 // ns -> key -> ttl
	deletes map[string]map[string]bool   // ns -> key -> true
	done    bool
}

// Put 缓冲一次写入；ttl 为有效期（tick 数），0 表示永不过期。
func (t *Tx) Put(ns, key string, value any, ttl uint64) error {
	if t.done {
		return ErrTxClosed
	}
	if t.writes[ns] == nil {
		t.writes[ns] = make(map[string]any)
		t.ttls[ns] = make(map[string]uint64)
	}
	t.writes[ns][key] = value
	t.ttls[ns][key] = ttl
	if t.deletes[ns] != nil {
		delete(t.deletes[ns], key)
	}
	return nil
}

// Delete 缓冲一次删除。
func (t *Tx) Delete(ns, key string) error {
	if t.done {
		return ErrTxClosed
	}
	if t.deletes[ns] == nil {
		t.deletes[ns] = make(map[string]bool)
	}
	t.deletes[ns][key] = true
	if t.writes[ns] != nil {
		delete(t.writes[ns], key)
		delete(t.ttls[ns], key)
	}
	return nil
}

// Commit 原子提交：先校验全部命名空间的写权限，任一越权则整个事务
// 被拒绝（不留下任何部分写入）；校验通过后按 (ns, key) 字典序分配
// 全局单调 Seq 并一次性生效，最后非阻塞地通知订阅者。
//
// 乱序/迟到写入策略：提交即获得全序 Seq，后到者覆盖先到者
// （last-writer-wins，按提交顺序），因此结果只取决于提交顺序，
// 而引擎保证提交顺序确定（按 NPC ID 排序），从而整体可复现。
func (t *Tx) Commit() error {
	if t.done {
		return ErrTxClosed
	}
	t.done = true

	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()

	acl := s.acls[t.writer]
	// 第一阶段：全量权限校验，失败则整体拒绝。
	for ns := range t.writes {
		if !acl.CanWrite(ns) {
			return ErrUnauthorized
		}
	}
	for ns := range t.deletes {
		if !acl.CanWrite(ns) {
			return ErrUnauthorized
		}
	}

	// 第二阶段：按确定顺序应用。
	type item struct{ ns, key string }
	items := make([]item, 0, len(t.writes)+len(t.deletes))
	for ns, kv := range t.writes {
		for k := range kv {
			items = append(items, item{ns, k})
		}
	}
	for ns, kv := range t.deletes {
		for k := range kv {
			items = append(items, item{ns, k})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].ns != items[j].ns {
			return items[i].ns < items[j].ns
		}
		return items[i].key < items[j].key
	})

	var changes []Change
	for _, it := range items {
		if t.deletes[it.ns][it.key] {
			if s.data[it.ns] != nil {
				delete(s.data[it.ns], it.key)
			}
			s.seq++
			changes = append(changes, Change{
				Fact:    Fact{Namespace: it.ns, Key: it.key, Writer: t.writer, Tick: s.tick, Seq: s.seq},
				Deleted: true,
			})
			continue
		}
		if s.data[it.ns] == nil {
			s.data[it.ns] = make(map[string]Fact)
		}
		s.seq++
		f := Fact{
			Namespace: it.ns,
			Key:       it.key,
			Value:     t.writes[it.ns][it.key],
			Writer:    t.writer,
			Tick:      s.tick,
			Seq:       s.seq,
			TTL:       t.ttls[it.ns][it.key],
		}
		s.data[it.ns][it.key] = f
		changes = append(changes, Change{Fact: f})
	}

	// 第三阶段：非阻塞通知，慢订阅者丢弃最旧事件，绝不阻塞提交。
	for _, sub := range s.subs {
		for _, c := range changes {
			if sub.prefix.Match(c.Fact.Namespace) {
				select {
				case sub.ch <- c:
				default:
					sub.dropped.Add(1)
				}
			}
		}
	}
	return nil
}

// Rollback 放弃全部缓冲写入；幂等。
func (t *Tx) Rollback() {
	if t.done {
		return
	}
	t.done = true
	t.writes = nil
	t.ttls = nil
	t.deletes = nil
}
