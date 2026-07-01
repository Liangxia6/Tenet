# Go Lock & Budget

> Session Lock + Fencing Token、Tool Rate Limit、Token Budget

---

## 1. Session-Workspace Lock

（待展开：SETNX + TTL 30s + 心跳续约。目录级排它锁的语义——同一 session 同一时间只有一个 Agent 写文件。获取/续约/释放的完整生命周期）

## 2. Fencing Token

（待展开：防止 GC 卡顿导致脑裂——每次锁获取/续约时 INCR fencing_token。Python 层和 Go 层文件操作前的 ValidateFencingToken 校验。token 不匹配 → 拒绝写入 → 自终止）

## 3. Tool Rate-Limit Lock

（待展开：Lua 原子令牌桶——INCR + EXPIRE + 阈值检查。配置驱动（yaml 中定义每个工具的 max_per_second / max_per_minute）。返回 (allowed, retryAfter)）

## 4. Redis Unavailability Fallback

（待展开：Redis 不可用时的降级策略——Session Lock 跳过（依赖 SQLite 串行写做最后防线）、Tool Rate Limit 回退到 Python 层本地内存限流）

## 5. Token Budget

（待展开：记账不决策——Guard Pattern 防重复记录、零 token 执行不记录。TokenUsed 事件写入 event_log、TokenProjection 聚合）

## 6. Lock Manager Interface

（待展开：AcquireSessionLock / RenewSessionLock / ReleaseSessionLock / ValidateFencingToken / CheckToolRateLimit 的完整签名和语义）
