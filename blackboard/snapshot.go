package blackboard

// Snapshot 是某一时刻、某一主体视角下的自洽只读视图。
// 快照在创建时完成拷贝与 ACL 过滤，读取方永远不会看到字段级混合的
// 中间状态，也不会看到越权命名空间。
type Snapshot struct {
	reader string
	tick   uint64
	facts  map[string]map[string]Fact // ns -> key -> fact
}

// Tick 返回快照对应的逻辑时钟。
func (s *Snapshot) Tick() uint64 { return s.tick }

// Get 读取某命名空间下的一个事实。
// 过期事实视为不存在（不参与决策）；越权命名空间返回 ErrUnauthorized。
func (s *Snapshot) Get(ns, key string) (Fact, error) {
	kv, ok := s.facts[ns]
	if !ok {
		// 区分“无权限”与“无数据”：快照只包含被授权命名空间，
		// 这里无法直接区分，统一按未找到处理；权限校验在 Store 层完成。
		return Fact{}, ErrNotFound
	}
	f, ok := kv[key]
	if !ok || f.Expired(s.tick) {
		return Fact{}, ErrNotFound
	}
	return f, nil
}

// All 返回某命名空间下全部未过期事实（按键排序，保证遍历确定）。
func (s *Snapshot) All(ns string) []Fact {
	kv, ok := s.facts[ns]
	if !ok {
		return nil
	}
	out := make([]Fact, 0, len(kv))
	for _, f := range kv {
		if !f.Expired(s.tick) {
			out = append(out, f)
		}
	}
	sortFacts(out)
	return out
}
