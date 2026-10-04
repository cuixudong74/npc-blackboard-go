package runtime

import (
	"sort"
	"sync"

	"github.com/cuixudong74/npc-blackboard-go/arbitration"
	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// Actuator 是行为执行回调：行为获胜后由引擎调用，
// 在事务内完成本周期对黑板的全部写入；返回错误将导致事务回滚。
type Actuator func(tx *blackboard.Tx, snap *blackboard.Snapshot) error

// Behavior 绑定一个仲裁候选与其执行体。
type Behavior struct {
	Candidate arbitration.Behavior
	Act       Actuator
}

// NPC 是一个参与协同决策的实体。行为可运行时动态增删，
// 引擎只在 tick 边界读取行为列表，进行中的决策不受影响。
type NPC struct {
	ID string

	mu        sync.RWMutex
	behaviors map[string]Behavior // 按行为 ID 索引
	current   string              // 当前正在执行的行为 ID
}

// NewNPC 创建空 NPC。
func NewNPC(id string) *NPC {
	return &NPC{ID: id, behaviors: make(map[string]Behavior)}
}

// AddBehavior 动态添加或替换行为（立即生效于下一 tick）。
func (n *NPC) AddBehavior(b Behavior) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.behaviors[b.Candidate.ID()] = b
}

// RemoveBehavior 动态移除行为；若移除的是当前行为，
// 下一 tick 仲裁会自动让位给其他适用行为。
func (n *NPC) RemoveBehavior(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.behaviors, id)
}

// Current 返回当前正在执行的行为 ID。
func (n *NPC) Current() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.current
}

// candidates 返回按行为 ID 排序的候选列表（遍历顺序确定）。
func (n *NPC) candidates() []arbitration.Behavior {
	n.mu.RLock()
	defer n.mu.RUnlock()
	ids := make([]string, 0, len(n.behaviors))
	for id := range n.behaviors {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]arbitration.Behavior, 0, len(ids))
	for _, id := range ids {
		out = append(out, n.behaviors[id].Candidate)
	}
	return out
}

// actuatorOf 返回某行为的执行体。
func (n *NPC) actuatorOf(id string) (Actuator, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	b, ok := n.behaviors[id]
	return b.Act, ok
}

// setCurrent 由引擎在提交成功后更新当前行为。
func (n *NPC) setCurrent(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.current = id
}
