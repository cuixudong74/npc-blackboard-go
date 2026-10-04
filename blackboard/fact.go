// Package blackboard 提供基于命名空间隔离、MVCC 快照与事务提交的
// 黑板事实存储，供多个 NPC 并发读写。
package blackboard

// Fact 是黑板上的一条事实。
type Fact struct {
	Namespace string // 所属命名空间（层级以 "/" 分隔，如 "world/enemy"）
	Key       string // 命名空间内唯一键
	Value     any    // 事实值（须可 JSON 序列化以便持久化）
	Writer    string // 写入者（NPC ID 或 "system"）
	Tick      uint64 // 写入发生的逻辑时钟（决策周期号）
	Seq       uint64 // 存储全局单调序号，用于乱序/迟到写入的确定裁决
	TTL       uint64 // 有效期（单位：tick），0 表示永不过期
}

// Expired 报告该事实在逻辑时钟 now 时是否已过期。
func (f Fact) Expired(now uint64) bool {
	if f.TTL == 0 {
		return false
	}
	return now >= f.Tick+f.TTL
}
