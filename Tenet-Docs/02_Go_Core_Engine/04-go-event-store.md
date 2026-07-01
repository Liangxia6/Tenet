# Go Event Store

> SQLite Event Store：表结构、写入幂等、事务控制、WriteDaemon

---

## 1. Database Schema

（待展开：event_log / sessions / snapshots / token_telemetry 四张表的完整 DDL、索引设计、约束说明）

## 2. Event Log Table

（待展开：字段详解——stream_id 树形结构、stream_seq 递增保证、event_type 枚举、payload JSON 格式、parent_id 谱系引用、timestamp ISO 8601）

## 3. Write Model: Append-Only + Immediate Commit

（待展开：Decide 事件的立即落盘语义、Record 事件的延迟批量提交、事务保证 seq 连续无空洞）

## 4. WriteDaemon: Single-Writer Goroutine

（待展开：全局单写守护协程设计——channel 排队、同步等待 resultCh、MaxOpenConns=1 约束。消除 SQLITE_BUSY）

## 5. Read Model

（待展开：Read 不做缓存——Event Store 是唯一权威、Projection 层负责缓存。范围查询接口、谱系查询（GetLineage/GetChildStreams））

## 6. Migration Strategy

（待展开：SQLite 迁移脚本管理、版本号追踪、回滚策略）
