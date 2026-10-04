# npc-blackboard-go

一套基于黑板机制的 NPC 协同决策运行时。多个 NPC 在同一次场景推进中共享同一份黑板事实，各自按不同的行为目标读写与订阅；行为选择随环境变化及时调整；固定随机种子与相同输入序列下结果完全一致，可复盘、可调参。

- 命名空间层级隔离 + 授权可见范围
- 不可变快照：读者任意时刻看到的都是自洽完整快照，无字段级混合
- 行为仲裁与抢占：优先级 / 适用条件 / 代价竞争，平局有明确裁决，结果确定可解释
- 决策周期事务：全部生效或全部回滚，无部分写入、无悬挂抢占态
- 事实携带写入时间与有效期，过期事实不参与决策；迟到 / 乱序写入有确定策略
- 确定性：固定种子 + 相同输入序列，逐帧行为选择与黑板演化完全一致，不受并发调度与遍历顺序影响
- 高并发无数据竞争（`-race` 通过），慢 NPC / 慢订阅者不阻塞整体决策，决策耗时有上界
- 运行时动态增删 NPC 与行为，决策不错位、不丢事件
- WAL + 检查点持久化，重启后从最近一次已提交状态继续

## 安装

```bash
go get github.com/cuixudong74/npc-blackboard-go
```

## 快速开始

```go
cfg := runtime.DefaultConfig()
cfg.Seed = 42
r, _ := runtime.New(cfg)
defer r.Close()

grants := blackboard.NewGrantSet(
    blackboard.Grant{Namespace: "/npc", Perm: blackboard.PermRead | blackboard.PermWrite, Recursive: true},
    blackboard.Grant{Namespace: "/world", Perm: blackboard.PermRead, Recursive: true},
)
r.AddNPC(runtime.NewNPC("guard", grants, patrolBehavior{}, fleeBehavior{}))

rep, _ := r.Step() // 一个决策周期
for _, d := range rep.Decisions {
    fmt.Println(d.NPCID, "->", d.WinnerID, d.Reason)
}
```

## 黑板语义

### 命名空间与层级

命名空间是斜杠分隔的层级路径，如 `/world/room/42`。根命名空间为 `/`。授权可以作用于某命名空间本身，也可以通过 `Recursive: true` 作用于其所有后代。

```go
blackboard.Grant{Namespace: "/npc", Perm: blackboard.PermRead | blackboard.PermWrite, Recursive: true}
```

### 不可变快照（快照隔离）

黑板只持有一个指向「已提交快照」的指针。快照一旦创建永不修改：

- 读者通过 `Blackboard.Snapshot()` 拿到当前不可变快照，或通过 `View` 做授权读取。
- 一次提交是原子的指针交换：读者要么看到旧的完整快照，要么看到新的完整快照，绝不会看到「同一条事实里字段 A 已更新、字段 B 还是旧值」的混合状态。
- 写入采用 copy-on-write：只复制受影响的命名空间映射，其余命名空间与旧快照共享。

### 授权

每个 NPC 持有一个 `GrantSet`。每次读取 / 写入都按命名空间校验权限：

- 读取未授权命名空间返回 `blackboard.ErrUnauthorized`（不会静默返回空）。
- 写入未授权命名空间会导致整个决策周期回滚（`runtime.ErrUnauthorizedWrite`）。

### 事实、有效期与过期

每条事实（`Fact`）携带：

| 字段 | 含义 |
| --- | --- |
| `WriteTick` | 写入时的逻辑帧（由运行时盖戳，行为不可伪造） |
| `ExpireTick` | 过期帧；`0` 表示永不过期 |
| `Writer` | 写入者 NPC ID |

`ExpireTick <= 当前帧` 的事实对读者不可见（物理上仍保留，直到检查点压缩）。

写入操作（`WriteOp`）可带 `TTL`（有效期帧数）；未指定时用 `Config.DefaultTTL`。

### 迟到写入与乱序更新

默认情况下写入的 `WriteTick` 等于当前帧。行为也可通过 `WriteOp.SourceTick` 把事实钉到更早的帧（例如迟到的事件）。提交时按每个 key 比较：

- 若已有事实的 `WriteTick > 本次写入的 WriteTick`，判定为**迟到写入**，由 `Config.LatePolicy` 处理：
  - `LateDrop`（默认）：丢弃，记入 `FrameReport.Dropped`。
  - `LateQuarantine`：重定向到 `/quarantine/...` 命名空间，原事实不动。
  - `LateOverwrite`：允许覆盖（不推荐，记入报告）。
- 同帧多个 NPC 写同一 key（`WriteTick` 相同）：按写入者 ID 字典序裁决，字典序大者胜，记入 `FrameReport.Conflicts`。
- `SourceTick` 晚于当前帧（未来写入）直接判定为错误，整个周期回滚。

## 仲裁规则

每个 NPC 在一个决策周期内至多有一个行为胜出。`behavior.Arbitrate` 对所有已注册行为按以下顺序裁决（先命中者胜）：

1. 只有 `Applicable(ctx)` 为真的行为参与竞争。
2. **优先级高者胜**（`Priority()`）。
3. 优先级相同：**代价低者胜**（`Cost(ctx)`，`CostEpsilon = 1e-9` 内视为平局）。
4. 代价相同：**行为 ID 字典序小者胜**。
5. 若胜者与当前正在执行的行为不同，发生**抢占**（`PreemptedFrom` 记录被抢占者，`Reason` 记录原因）。
6. 若没有任何行为适用，NPC **空闲**，当前行为被抢占但无胜者。

每次仲裁都会返回确定的 `Decision`：胜者 ID、原因（`Reason`，可解释）、所有候选、以及每个竞争失败行为的裁决规则（`Conflicts`）。

抢占状态（当前执行哪个行为）本身作为黑板事实存储在 `/runtime/active:<npcID>`，与 NPC 写入处于同一个事务：周期回滚时抢占状态自动还原，不会留下悬挂的抢占态。

## 决策周期（事务）

`Runtime.Step()` 执行一个完整决策周期：

1. 应用排队的注册表变更（在帧边界生效）。
2. 帧号 `tick = 上次提交 + 1`，取当前不可变快照为基线。
3. 取出本帧到期事件。
4. 并发评估：每个 NPC 用**同一个基线快照**独立仲裁与执行，产出写入集。
5. 合并所有写入：校验授权、盖戳、TTL、迟到策略、同帧冲突，生成变更集。
6. 原子提交（指针交换），推进帧号。
7. 写 WAL；按 `CheckpointInterval` 做检查点。
8. 非阻塞地推送给订阅者。

**全有或全无**：任一行为返回错误、写入越权、未来写入或帧超时，整个周期回滚——黑板停在上一个已提交帧，帧号不前进，事件不消费，抢占状态还原。

## 确定性

- 随机数：`behavior.NewFrameRNG(seed, frame, npcID)` 从 `(seed, frame, npcID)` 用 FNV 派生，与 NPC 调度顺序无关。
- 遍历顺序：所有 map 迭代都排序（命名空间、key、NPC ID、行为 ID）。
- 帧号是逻辑时钟，不使用墙钟时间做决策；TTL 按帧计算。
- 并发只影响速度，不影响结果：评估结果按 NPC ID 排序后合并。

验证：`TestDeterminism` 用 WorkerCount=1 与 WorkerCount=8 跑同一脚本，逐帧胜者与写入完全一致。

## 并发模型与耗时上界

- NPC 评估在 `Config.WorkerCount` 个 worker 上并发进行，读取同一个不可变快照，无共享可变状态，无数据竞争（`go test -race` 通过）。
- 慢订阅者：订阅通道有缓冲，推送是非阻塞的；通道满时丢弃并计数（`Subscriber.Dropped()`），绝不阻塞帧循环。
- 帧超时：`Config.FrameTimeout` 是一个决策周期的墙钟上界。超时后运行时停止等待并回滚。

**决策耗时上界**：单次 `Step()` 的最坏耗时 = `FrameTimeout` + `O(N)` 提交开销（copy-on-write，N 为命名空间数量）。行为通过 `Context.Cancel` 感知取消；若行为忽略取消，运行时不会强杀 goroutine（Go 无法安全强杀），但会在超时后立即返回并回滚，遗留 goroutine 只读不可变快照、写私有通道，不影响后续帧。

可调参数：

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `Seed` | 1 | 主随机种子 |
| `WorkerCount` | 4 | 并发 NPC 评估 worker 数 |
| `FrameTimeout` | 1s | 单帧墙钟上界 |
| `FrameInterval` | 0 | `Run` 循环目标帧间隔，0 表示尽快 |
| `DefaultTTL` | 0 | 事实默认有效期（帧），0 表示永不过期 |
| `LatePolicy` | `LateDrop` | 迟到写入策略 |
| `QueueCapacity` | 1024 | 事件队列容量 |
| `SubscribeBuffer` | 256 | 订阅通道缓冲 |
| `CheckpointInterval` | 100 | 检查点间隔（帧） |
| `PersistPath` | "" | 持久化目录，空表示纯内存 |

## 动态增删

- `AddNPC` / `RemoveNPC` / `AddBehavior` / `RemoveBehavior` 都是线程安全的，变更在**下一个帧边界**生效。
- 进行中的决策使用帧开始时的注册表快照，不会错位或丢失事件。
- 事件在入队时分配全局单调序号；投递给不存在的 NPC 时不会致命（事件按序号处理，目标缺失则跳过）。

## 重启与持久化

设置 `PersistPath` 后：

- 每次提交追加一条 WAL 记录（已投递事件 + 写入）。
- 入队事件立即追加 WAL（保证未投递事件不丢）。
- 每 `CheckpointInterval` 帧写一次快照检查点（原子写 + 重命名），并截断旧 WAL。
- 重启时 `New` 自动加载最近检查点并重放之后的 WAL，恢复黑板快照、帧号与未投递事件。

**注意**：NPC 是代码（行为逻辑）而非数据，重启后需重新注册；黑板状态（事实、帧号、未投递事件）自动恢复。

值序列化采用类型保持包装器（`persist/jval`）：`int` 重启后仍是 `int`，不会变成 `float64`，保证行为在重启前后做出一致判断。

## 测试

```bash
go test -race ./...
```

覆盖：

- 并发读写（`-race`）
- 仲裁冲突（优先级 / 代价 / 字典序 / 抢占 / 空闲）
- 周期回滚（行为错误、越权、超时）
- 事实过期与乱序（TTL、迟到丢弃、隔离、未来写入）
- 确定性（不同 worker 数结果一致）
- 动态增删（NPC / 行为）
- 慢订阅者不阻塞
- 重启（连续运行 vs 中断重启后逐帧一致、未投递事件恢复、复合类型保持）

## 包结构

```
blackboard/   命名空间、授权、事实、不可变快照、变更集（copy-on-write）
behavior/     行为接口、决策上下文、确定性 RNG、仲裁器
runtime/      帧循环、NPC 注册表、事件队列、订阅、配置
persist/      WAL + 检查点、重启回放
examples/     端到端示例
```
