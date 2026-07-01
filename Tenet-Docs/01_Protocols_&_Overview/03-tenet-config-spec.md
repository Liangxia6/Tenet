# Tenet Configuration Specification

> 静态与动态 YAML 配置文件规范

---

## 1. Static Config (`tenet.yaml`)

（待展开：完整 YAML 字段规范——SQLite 路径、Redis 地址、gRPC 端口、Workflow 默认参数（快照间隔/超时/重试/并发上限）、工具限频配置、LLM Provider 列表）

## 2. Dynamic Config (Event-Log Based)

（待展开：通过 event_log 中的配置变更事件实现动态配置——Workflow 版本号变更、策略路由规则更新、限频参数调整。保证配置变更可追溯）

## 3. Config Loading Order

（待展开：启动时的加载顺序——yaml 文件 → 环境变量覆盖 → event_log 动态配置 → 最终生效配置）

## 4. Config Validation

（待展开：配置校验规则——必填字段、类型检查、值域约束、启动时 Fail-Fast 策略）
