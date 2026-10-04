package runtime

import (
	"encoding/json"
	"os"

	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// persistedState 是引擎的可序列化检查点。
type persistedState struct {
	Board   blackboard.State  `json:"board"`
	Current map[string]string `json:"current"` // NPC ID -> 当前行为 ID
	Seed    int64             `json:"seed"`
}

// Save 将最近一次已提交状态（黑板 + 各 NPC 当前行为）写入 path。
// 只包含已提交数据，未提交事务不受影响也不会被保存。
func (e *Engine) Save(path string) error {
	e.mu.Lock()
	cur := make(map[string]string, len(e.npcs))
	for id, n := range e.npcs {
		cur[id] = n.Current()
	}
	ps := persistedState{
		Board:   e.store.Export(),
		Current: cur,
		Seed:    e.seed,
	}
	e.mu.Unlock()

	data, err := json.MarshalIndent(ps, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// Load 从 path 恢复最近一次已提交状态：
// 黑板事实、逻辑时钟、全局序号与 ACL 全部还原；
// 已注册 NPC 的当前行为也被还原，保证重启后决策不错位。
// 行为与执行体不可序列化，需在 Load 前后重新注册 NPC 及其行为。
func (e *Engine) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		return err
	}
	e.store.Import(ps.Board)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.tick = ps.Board.Tick
	for id, cur := range ps.Current {
		if n, ok := e.npcs[id]; ok {
			n.setCurrent(cur)
		}
	}
	return nil
}
