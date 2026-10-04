package blackboard

import "errors"

// ErrNotFound 表示事实不存在或已过期。
var ErrNotFound = errors.New("blackboard: fact not found")

// sortFacts 按 (Namespace, Key) 排序，保证遍历顺序确定。
func sortFacts(fs []Fact) {
	for i := 1; i < len(fs); i++ {
		for j := i; j > 0 && lessFact(fs[j], fs[j-1]); j-- {
			fs[j], fs[j-1] = fs[j-1], fs[j]
		}
	}
}

func lessFact(a, b Fact) bool {
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	return a.Key < b.Key
}

// ErrTxClosed 表示事务已提交或回滚后仍被使用。
var ErrTxClosed = errors.New("blackboard: transaction already closed")
