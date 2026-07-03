package worker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpenAIClientGenerateThoughtTextResponse(t *testing.T) {
	var captured struct {
		Model    string          `json:"model"`
		Messages []openAIMessage `json:"messages"`
		Tools    []any           `json:"tools"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("authorization header = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}
		}`))
	}))
	defer server.Close()

	client, err := NewOpenAIClient(OpenAIConfig{
		BaseURL: server.URL,
		APIKey:  "test-key",
		Model:   "test-model",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := client.GenerateThought(t.Context(), GenerateThoughtRequest{
		SystemPrompt: "system",
		Messages:     []Message{{Role: "user", Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if captured.Model != "test-model" {
		t.Fatalf("model = %q, want test-model", captured.Model)
	}
	if len(captured.Messages) != 2 || captured.Messages[0].Role != "system" || captured.Messages[1].Role != "user" {
		t.Fatalf("messages = %+v", captured.Messages)
	}
	if resp.Thought != "done" || !resp.IsFinal {
		t.Fatalf("response = %+v", resp)
	}
	if resp.Usage.TotalTokens != 7 {
		t.Fatalf("usage = %+v", resp.Usage)
	}
}

func TestOpenAIClientGenerateThoughtToolCallResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var captured struct {
			Tools []openAIToolWrapper `json:"tools"`
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(captured.Tools) != 1 || captured.Tools[0].Function.Name != "read_file" {
			t.Fatalf("tools = %+v", captured.Tools)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"choices":[{
				"message":{
					"role":"assistant",
					"content":"",
					"tool_calls":[{
						"id":"call_1",
						"type":"function",
						"function":{"name":"read_file","arguments":"{\"path\":\"README.md\"}"}
					}]
				},
				"finish_reason":"tool_calls"
			}]
		}`))
	}))
	defer server.Close()

	client, err := NewOpenAIClient(OpenAIConfig{
		BaseURL: server.URL,
		APIKey:  "test-key",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := client.GenerateThought(t.Context(), GenerateThoughtRequest{
		Messages: []Message{{Role: "user", Content: "read file"}},
		Tools: []ToolDefinition{{
			Name:             "read_file",
			Description:      "Read a file.",
			ParametersSchema: `{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`,
		}},
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if resp.IsFinal {
		t.Fatalf("expected non-final tool call response")
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("tool calls = %+v", resp.ToolCalls)
	}
	call := resp.ToolCalls[0]
	if call.CallID != "call_1" || call.ToolName != "read_file" || call.Arguments != `{"path":"README.md"}` {
		t.Fatalf("tool call = %+v", call)
	}
}

func TestDeepSeekClientDefaults(t *testing.T) {
	client, err := NewDeepSeekClient(OpenAIConfig{APIKey: "deepseek-key"})
	if err != nil {
		t.Fatalf("new deepseek client: %v", err)
	}
	if client.baseURL != DefaultDeepSeekBaseURL {
		t.Fatalf("baseURL = %q, want %q", client.baseURL, DefaultDeepSeekBaseURL)
	}
	if client.model != DefaultDeepSeekModel {
		t.Fatalf("model = %q, want %q", client.model, DefaultDeepSeekModel)
	}
}
