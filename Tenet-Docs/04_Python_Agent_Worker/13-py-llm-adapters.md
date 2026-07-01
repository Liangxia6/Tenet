# Python LLM Adapters

> LLM Provider 适配器（OpenAI/Anthropic/DeepSeek）

---

## 1. Provider Interface

（待展开：统一的 Provider 抽象——chat(messages, tools, **kwargs) → (text, tool_calls, usage)。支持同步和流式两种模式）

## 2. OpenAI Adapter

（待展开：OpenAI 兼容 API 的适配实现——请求格式映射、tool_use 解析、流式 token 处理、错误码映射）

## 3. Anthropic Adapter

（待展开：Anthropic Messages API 适配——system prompt 分离、tool_use content block 解析、stop_reason 处理）

## 4. DeepSeek Adapter

（待展开：DeepSeek API 适配——OpenAI 兼容模式、特殊参数处理、rate limit 适配）

## 5. Provider Selection & Failover

（待展开：Provider 选择逻辑（配置驱动）、主备切换、不可用 Provider 的自动降级）

## 6. Token Usage Reporting

（待展开：每次 LLM 调用后提取 token 用量（prompt_tokens/completion_tokens）、通过 gRPC 回传 TokenUsed 事件、成本计算）
