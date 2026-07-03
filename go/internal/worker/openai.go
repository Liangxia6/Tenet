package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	DefaultOpenAIBaseURL   = "https://api.openai.com/v1"
	DefaultOpenAIModel     = "gpt-4o-mini"
	DefaultDeepSeekBaseURL = "https://api.deepseek.com"
	DefaultDeepSeekModel   = "deepseek-v4-flash"
)

type OpenAIConfig struct {
	BaseURL    string
	APIKey     string
	Model      string
	HTTPClient *http.Client
}

type OpenAIClient struct {
	baseURL    string
	apiKey     string
	model      string
	httpClient *http.Client
	startedAt  time.Time
}

func NewOpenAIClient(cfg OpenAIConfig) (*OpenAIClient, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		baseURL = DefaultOpenAIBaseURL
	}
	apiKey := strings.TrimSpace(cfg.APIKey)
	if apiKey == "" {
		return nil, errors.New("openai api key is required")
	}
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		model = DefaultOpenAIModel
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 120 * time.Second}
	}
	return &OpenAIClient{
		baseURL:    baseURL,
		apiKey:     apiKey,
		model:      model,
		httpClient: httpClient,
		startedAt:  time.Now(),
	}, nil
}

func NewOpenAIClientFromEnv(model string) (*OpenAIClient, error) {
	return NewOpenAIClient(OpenAIConfig{
		BaseURL: os.Getenv("OPENAI_BASE_URL"),
		APIKey:  os.Getenv("OPENAI_API_KEY"),
		Model:   model,
	})
}

func NewDeepSeekClient(cfg OpenAIConfig) (*OpenAIClient, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = DefaultDeepSeekBaseURL
	}
	if strings.TrimSpace(cfg.Model) == "" {
		cfg.Model = DefaultDeepSeekModel
	}
	return NewOpenAIClient(cfg)
}

func NewDeepSeekClientFromEnv(model string) (*OpenAIClient, error) {
	return NewDeepSeekClient(OpenAIConfig{
		BaseURL: os.Getenv("DEEPSEEK_BASE_URL"),
		APIKey:  os.Getenv("DEEPSEEK_API_KEY"),
		Model:   model,
	})
}

func (c *OpenAIClient) GenerateThought(ctx context.Context, req GenerateThoughtRequest) (GenerateThoughtResponse, error) {
	model := strings.TrimSpace(req.Model)
	if model == "" {
		model = c.model
	}
	body := openAIChatRequest{
		Model:       model,
		Temperature: req.Temperature,
		Messages:    openAIMessages(req),
		Tools:       openAITools(req.Tools),
	}
	data, err := json.Marshal(body)
	if err != nil {
		return GenerateThoughtResponse{}, fmt.Errorf("marshal openai request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(data))
	if err != nil {
		return GenerateThoughtResponse{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return GenerateThoughtResponse{}, err
	}
	defer httpResp.Body.Close()
	respData, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return GenerateThoughtResponse{}, err
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return GenerateThoughtResponse{}, fmt.Errorf("openai request failed: status=%d body=%s", httpResp.StatusCode, string(respData))
	}
	var decoded openAIChatResponse
	if err := json.Unmarshal(respData, &decoded); err != nil {
		return GenerateThoughtResponse{}, fmt.Errorf("decode openai response: %w", err)
	}
	if len(decoded.Choices) == 0 {
		return GenerateThoughtResponse{}, errors.New("openai response has no choices")
	}
	choice := decoded.Choices[0]
	toolCalls := make([]ToolCall, 0, len(choice.Message.ToolCalls))
	for _, call := range choice.Message.ToolCalls {
		toolCalls = append(toolCalls, ToolCall{
			CallID:    call.ID,
			ToolName:  call.Function.Name,
			Arguments: call.Function.Arguments,
		})
	}
	return GenerateThoughtResponse{
		Thought:      choice.Message.Content,
		ToolCalls:    toolCalls,
		IsFinal:      len(toolCalls) == 0 && choice.FinishReason != "tool_calls",
		FinishReason: choice.FinishReason,
		Usage: TokenUsage{
			PromptTokens:     decoded.Usage.PromptTokens,
			CompletionTokens: decoded.Usage.CompletionTokens,
			TotalTokens:      decoded.Usage.TotalTokens,
		},
	}, nil
}

func (c *OpenAIClient) ExecuteTool(context.Context, ExecuteToolRequest) (ExecuteToolResponse, error) {
	return ExecuteToolResponse{}, errors.New("openai client does not execute tools directly; wrap it with LocalAgentClient")
}

func (c *OpenAIClient) HealthCheck(context.Context) (HealthCheckResponse, error) {
	return HealthCheckResponse{
		Status:        "SERVING",
		WorkerCount:   1,
		UptimeSeconds: int64(time.Since(c.startedAt).Seconds()),
	}, nil
}

type openAIChatRequest struct {
	Model       string              `json:"model"`
	Temperature float64             `json:"temperature,omitempty"`
	Messages    []openAIMessage     `json:"messages"`
	Tools       []openAIToolWrapper `json:"tools,omitempty"`
}

type openAIMessage struct {
	Role       string           `json:"role"`
	Content    string           `json:"content,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
}

type openAIToolWrapper struct {
	Type     string         `json:"type"`
	Function openAIFunction `json:"function"`
}

type openAIFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type openAIToolCall struct {
	ID       string             `json:"id,omitempty"`
	Type     string             `json:"type,omitempty"`
	Function openAIToolFunction `json:"function"`
}

type openAIToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type openAIChatResponse struct {
	Choices []struct {
		Message      openAIMessage `json:"message"`
		FinishReason string        `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

func openAIMessages(req GenerateThoughtRequest) []openAIMessage {
	messages := make([]openAIMessage, 0, len(req.Messages)+1)
	if strings.TrimSpace(req.SystemPrompt) != "" {
		messages = append(messages, openAIMessage{Role: "system", Content: req.SystemPrompt})
	}
	for _, msg := range req.Messages {
		messages = append(messages, openAIMessage{
			Role:       msg.Role,
			Content:    msg.Content,
			ToolCallID: msg.ToolCallID,
			ToolCalls:  openAIToolCalls(msg.ToolCalls),
		})
	}
	return messages
}

func openAIToolCalls(calls []ToolCall) []openAIToolCall {
	if len(calls) == 0 {
		return nil
	}
	out := make([]openAIToolCall, 0, len(calls))
	for _, call := range calls {
		out = append(out, openAIToolCall{
			ID:   call.CallID,
			Type: "function",
			Function: openAIToolFunction{
				Name:      call.ToolName,
				Arguments: call.Arguments,
			},
		})
	}
	return out
}

func openAITools(tools []ToolDefinition) []openAIToolWrapper {
	if len(tools) == 0 {
		return nil
	}
	out := make([]openAIToolWrapper, 0, len(tools))
	for _, tool := range tools {
		schema := json.RawMessage(tool.ParametersSchema)
		if !json.Valid(schema) {
			schema = json.RawMessage(`{"type":"object","properties":{}}`)
		}
		out = append(out, openAIToolWrapper{
			Type: "function",
			Function: openAIFunction{
				Name:        tool.Name,
				Description: tool.Description,
				Parameters:  schema,
			},
		})
	}
	return out
}
