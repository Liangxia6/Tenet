package worker

import (
	"context"
	"time"
)

type LocalAgentClient struct {
	Generator Client
	Tools     *LocalToolExecutor
	StartedAt time.Time
}

func NewLocalAgentClient(generator Client, workspace string, dangerousPatterns []string) *LocalAgentClient {
	return NewLocalAgentClientWithAllowlist(generator, workspace, dangerousPatterns, nil)
}

func NewLocalAgentClientWithAllowlist(generator Client, workspace string, dangerousPatterns []string, toolAllowlist []string) *LocalAgentClient {
	if generator == nil {
		generator = NewEchoClient()
	}
	return &LocalAgentClient{
		Generator: generator,
		Tools:     NewLocalToolExecutorWithAllowlist(workspace, dangerousPatterns, toolAllowlist),
		StartedAt: time.Now(),
	}
}

func (c *LocalAgentClient) GenerateThought(ctx context.Context, req GenerateThoughtRequest) (GenerateThoughtResponse, error) {
	return c.Generator.GenerateThought(ctx, req)
}

func (c *LocalAgentClient) ExecuteTool(ctx context.Context, req ExecuteToolRequest) (ExecuteToolResponse, error) {
	return c.Tools.Execute(ctx, req), nil
}

func (c *LocalAgentClient) HealthCheck(ctx context.Context) (HealthCheckResponse, error) {
	if c.Generator != nil {
		if response, err := c.Generator.HealthCheck(ctx); err == nil {
			return response, nil
		}
	}
	return HealthCheckResponse{
		Status:        "SERVING",
		WorkerCount:   1,
		UptimeSeconds: int64(time.Since(c.StartedAt).Seconds()),
	}, nil
}
