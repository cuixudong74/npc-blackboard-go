package blackboard

// State 是黑板某一已提交时刻的完整可序列化状态，
// 用于重启后从最近一次已提交状态继续。
type State struct {
	Tick  uint64         `json:"tick"`
	Seq   uint64         `json:"seq"`
	Facts []Fact         `json:"facts"`
	ACLs  map[string]ACL `json:"acls"`
}

// Export 导出当前已提交状态（不含未提交的事务缓冲）。
// 事实按 (Namespace, Key) 排序，导出结果确定。
func (s *Store) Export() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := State{
		Tick: s.tick,
		Seq:  s.seq,
		ACLs: make(map[string]ACL, len(s.acls)),
	}
	for subj, acl := range s.acls {
		st.ACLs[subj] = acl
	}
	for _, ns := range s.namespaceNamesLocked() {
		kv := s.data[ns]
		keys := make([]string, 0, len(kv))
		for k := range kv {
			keys = append(keys, k)
		}
		sortStrings(keys)
		for _, k := range keys {
			st.Facts = append(st.Facts, kv[k])
		}
	}
	return st
}

// Import 用已提交状态覆盖当前存储（重启恢复用）。
// 导入后逻辑时钟与全局序号与导出端一致，后续提交的全序保持连续。
func (s *Store) Import(st State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tick = st.Tick
	s.seq = st.Seq
	s.data = make(map[string]map[string]Fact)
	for _, f := range st.Facts {
		if s.data[f.Namespace] == nil {
			s.data[f.Namespace] = make(map[string]Fact)
		}
		s.data[f.Namespace][f.Key] = f
	}
	s.acls = make(map[string]ACL, len(st.ACLs))
	for subj, acl := range st.ACLs {
		s.acls[subj] = acl
	}
}

func (s *Store) namespaceNamesLocked() []string {
	names := make([]string, 0, len(s.data))
	for ns := range s.data {
		names = append(names, ns)
	}
	sortStrings(names)
	return names
}

func sortStrings(xs []string) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j] < xs[j-1]; j-- {
			xs[j], xs[j-1] = xs[j-1], xs[j]
		}
	}
}
