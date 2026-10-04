package arbitration

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cuixudong74/npc-blackboard-go/blackboard"
)

// Decision 是一次仲裁的结果，包含胜负与可解释的原因。
type Decision struct {
	Winner    string   // 获胜行为 ID；无适用行为时为空
	Preempted string   // 被抢占的行为 ID（若有）
	Ranked    []string // 全部适用行为按裁决规则排序后的 ID 序列
	Reason    string   // 人类可读的裁决说明
}

// Arbiter 按确定性规则仲裁行为候选。
//
// 裁决规则（依次比较，完全确定，与候选传入顺序无关）：
//  1. 过滤掉在当前快照下不适用的候选；
//  2. 优先级高者胜；
//  3. 优先级相同，代价低者胜；
//  4. 仍平局，行为 ID 字典序小者胜（确定性平局裁决）。
//
// 抢占规则：当前行为与新胜者不同，且胜者优先级严格更高时，
// 当前行为被抢占；同优先级替换不发生（避免抖动）；
// 当前行为不再适用时直接让位，记为失效而非抢占。
type Arbiter struct{}

// New 创建仲裁器。
func New() *Arbiter { return &Arbiter{} }

type ranked struct {
	id   string
	prio int
	cost float64
}

// Arbitrate 对 candidates 进行仲裁；current 为当前正在执行的行为 ID（可为空）。
func (a *Arbiter) Arbitrate(snap *blackboard.Snapshot, current string, candidates []Behavior) Decision {
	// 第一阶段：过滤与评估。候选先按 ID 排序再评估，
	// 保证评估顺序与传入顺序无关。
	sorted := make([]Behavior, len(candidates))
	copy(sorted, candidates)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID() < sorted[j].ID() })

	var fit []ranked
	prioOf := make(map[string]int, len(sorted))
	for _, b := range sorted {
		prioOf[b.ID()] = b.Priority()
		if b.Applicable(snap) {
			fit = append(fit, ranked{id: b.ID(), prio: b.Priority(), cost: b.Cost(snap)})
		}
	}
	if len(fit) == 0 {
		return Decision{Reason: "no applicable behavior"}
	}

	// 第二阶段：确定性排序（优先级降序、代价升序、ID 升序）。
	sort.Slice(fit, func(i, j int) bool {
		if fit[i].prio != fit[j].prio {
			return fit[i].prio > fit[j].prio
		}
		if fit[i].cost != fit[j].cost {
			return fit[i].cost < fit[j].cost
		}
		return fit[i].id < fit[j].id
	})

	rankedIDs := make([]string, len(fit))
	for i, r := range fit {
		rankedIDs[i] = r.id
	}
	top := fit[0]

	d := Decision{Ranked: rankedIDs}
	switch {
	case current == "":
		d.Winner = top.id
		d.Reason = fmt.Sprintf("no current behavior; %s wins (prio=%d cost=%g)", top.id, top.prio, top.cost)
	case current == top.id:
		d.Winner = current
		d.Reason = fmt.Sprintf("current behavior %s keeps execution (prio=%d cost=%g)", current, top.prio, top.cost)
	case !knownCurrent(current, sorted):
		// current 已不在候选集中（行为被动态移除）：直接让位。
		d.Winner = top.id
		d.Reason = fmt.Sprintf("current behavior %s removed; %s takes over", current, top.id)
	case !applicable(current, sorted, snap):
		// 当前行为条件失效：直接让位，记为失效而非抢占。
		d.Winner = top.id
		d.Reason = fmt.Sprintf("current behavior %s no longer applicable; %s takes over", current, top.id)
	case top.prio > prioOf[current]:
		d.Winner = top.id
		d.Preempted = current
		d.Reason = fmt.Sprintf("%s preempts %s (prio %d > %d)", top.id, current, top.prio, prioOf[current])
	default:
		// 同优先级或更低优先级不抢占：当前行为继续。
		d.Winner = current
		d.Reason = fmt.Sprintf("current behavior %s continues (candidate %s prio %d <= %d)",
			current, top.id, top.prio, prioOf[current])
	}
	return d
}

func knownCurrent(current string, candidates []Behavior) bool {
	for _, b := range candidates {
		if b.ID() == current {
			return true
		}
	}
	return false
}

func applicable(current string, candidates []Behavior, snap *blackboard.Snapshot) bool {
	for _, b := range candidates {
		if b.ID() == current {
			return b.Applicable(snap)
		}
	}
	return false
}

// Explain 将裁决结果格式化为单行说明，便于日志与复盘。
func (d Decision) Explain() string {
	var b strings.Builder
	b.WriteString("winner=")
	if d.Winner == "" {
		b.WriteString("<none>")
	} else {
		b.WriteString(d.Winner)
	}
	if d.Preempted != "" {
		b.WriteString(" preempted=" + d.Preempted)
	}
	b.WriteString(" ranked=[" + strings.Join(d.Ranked, ",") + "]")
	b.WriteString(" reason: " + d.Reason)
	return b.String()
}
