package blackboard

import "strings"

// Scope 表示一个命名空间前缀（层级授权按前缀匹配）。
type Scope string

// Match 报告 ns 是否落在该授权范围内（自身或子层级）。
func (s Scope) Match(ns string) bool {
	p := string(s)
	if p == "" || p == "/" {
		return true
	}
	return ns == p || strings.HasPrefix(ns, p+"/")
}

// ACL 记录某个主体可读的命名空间范围集合。
// 写入权限 = 读取权限 + 主体只能写自己被授权的命名空间。
type ACL struct {
	Read  []Scope
	Write []Scope
}

// CanRead 报告是否允许读取命名空间 ns。
func (a ACL) CanRead(ns string) bool {
	for _, s := range a.Read {
		if s.Match(ns) {
			return true
		}
	}
	return false
}

// CanWrite 报告是否允许写入命名空间 ns。
func (a ACL) CanWrite(ns string) bool {
	for _, s := range a.Write {
		if s.Match(ns) {
			return true
		}
	}
	return false
}
