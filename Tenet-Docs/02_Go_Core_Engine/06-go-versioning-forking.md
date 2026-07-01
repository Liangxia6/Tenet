# Go Versioning & Forking

> GetVersion 版本控制、Fork 分支路由、谱系查询

---

## 1. Version Control: GetVersion

（待展开：GetVersion 的设计——命名变更点而非全局版本号。首次执行写 VersionMarker、回放时从 event_log 读版本号。多变更独立共存）

## 2. Non-Determinism Detection

（待展开：确定性检查失败时 panic 的完整信息——期望类型 vs 实际类型、stream_id、historyPos、引导开发者加 GetVersion）

## 3. Physical Stream ID Tree

（待展开：stream_id 的树形命名规范——`task:<uuid>` / `task:<uuid>/fork:<N>` / `task:<uuid>/fork:<N>/replay:<R>`。谱系编码在 ID 中的设计意图）

## 4. Fork Operation

（待展开：Fork 的完整流程——读取原流前 N 个事件、物理复制到新流、Config 初始化（historyPos=N）、新流独立执行。物理复制 vs 逻辑引用的取舍）

## 5. Fork Use Cases

（待展开：What-If 探索——从决策点分叉看不同结果。错误恢复——第 K 步出错后修正。两条分支独立，原分支保留用于事后分析）

## 6. Lineage Queries

（待展开：GetLineage——沿 parent_id 递归返回完整谱系链。GetChildStreams——LIKE 前缀查询所有子分支。Web UI 的流树可视化）
