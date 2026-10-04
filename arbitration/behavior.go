// Package arbitration 提供行为候选的确定性仲裁与抢占。
package arbitration

import "github.com/cuixudong74/npc-blackboard-go/blackboard"

// Behavior 是一个可竞争 NPC 执行权的行为候选。
type Behavior interface {
	ID() string
	// Priority 返回静态优先级，数值大者优先。
	Priority() int
	// Applicable 判断在快照视角下当前是否适用。
	Applicable(snap *blackboard.Snapshot) bool
	// Cost 返回在快照视角下的执行代价，代价小者优先。
	Cost(snap *blackboard.Snapshot) float64
}

// StaticBehavior 便于测试与配置的静态行为实现。
type StaticBehavior struct {
	Name      string
	Prio      int
	Condition func(snap *blackboard.Snapshot) bool
	CostFn    func(snap *blackboard.Snapshot) float64
}

func (b StaticBehavior) ID() string    { return b.Name }
func (b StaticBehavior) Priority() int { return b.Prio }
func (b StaticBehavior) Applicable(snap *blackboard.Snapshot) bool {
	if b.Condition == nil {
		return true
	}
	return b.Condition(snap)
}
func (b StaticBehavior) Cost(snap *blackboard.Snapshot) float64 {
	if b.CostFn == nil {
		return 0
	}
	return b.CostFn(snap)
}
