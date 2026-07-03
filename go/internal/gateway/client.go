package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	tenetv1 "github.com/tenet/orchestrator/internal/gateway/gen/tenet/v1"
	"github.com/tenet/orchestrator/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	workerServiceName       = "tenet.v1.TenetWorker"
	generateThoughtMethod   = "/" + workerServiceName + "/GenerateThought"
	executeToolMethod       = "/" + workerServiceName + "/ExecuteTool"
	workerHealthCheckMethod = "/" + workerServiceName + "/HealthCheck"
)

type ClientOptions struct {
	Address               string
	Registry              *WorkerRegistry
	ControlTimeout        time.Duration
	ExecuteTimeout        time.Duration
	RetryMaxAttempts      int
	RetryBackoffBase      time.Duration
	CircuitBreakerFailMax int
	CircuitBreakerTimeout time.Duration
}

type WorkerClient struct {
	opts    ClientOptions
	direct  string
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	breaker *CircuitBreaker
}

func NewWorkerClient(opts ClientOptions) (*WorkerClient, error) {
	if opts.Address == "" && opts.Registry == nil {
		return nil, errors.New("worker address or registry is required")
	}
	if opts.ControlTimeout <= 0 {
		opts.ControlTimeout = 60 * time.Second
	}
	if opts.ExecuteTimeout <= 0 {
		opts.ExecuteTimeout = 300 * time.Second
	}
	if opts.RetryMaxAttempts <= 0 {
		opts.RetryMaxAttempts = 3
	}
	if opts.RetryBackoffBase <= 0 {
		opts.RetryBackoffBase = time.Second
	}
	if opts.CircuitBreakerFailMax <= 0 {
		opts.CircuitBreakerFailMax = 5
	}
	if opts.CircuitBreakerTimeout <= 0 {
		opts.CircuitBreakerTimeout = 30 * time.Second
	}
	return &WorkerClient{
		opts:    opts,
		direct:  opts.Address,
		conns:   map[string]*grpc.ClientConn{},
		breaker: NewCircuitBreaker(opts.CircuitBreakerFailMax, opts.CircuitBreakerTimeout),
	}, nil
}

func (c *WorkerClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for address, conn := range c.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = fmt.Errorf("close %s: %w", address, err)
		}
	}
	c.conns = map[string]*grpc.ClientConn{}
	return first
}

func (c *WorkerClient) GenerateThought(ctx context.Context, req worker.GenerateThoughtRequest) (worker.GenerateThoughtResponse, error) {
	var out *tenetv1.GenerateThoughtResponse
	err := c.invokeWithWorker(ctx, c.opts.ExecuteTimeout, func(ctx context.Context, conn *grpc.ClientConn) error {
		var err error
		out, err = tenetv1.NewTenetWorkerClient(conn).GenerateThought(ctx, toProtoGenerateThoughtRequest(req))
		return err
	})
	if out == nil {
		return worker.GenerateThoughtResponse{}, err
	}
	return fromProtoGenerateThoughtResponse(out), err
}

func (c *WorkerClient) ExecuteTool(ctx context.Context, req worker.ExecuteToolRequest) (worker.ExecuteToolResponse, error) {
	var out *tenetv1.ExecuteToolResponse
	err := c.invokeWithWorker(ctx, c.opts.ExecuteTimeout, func(ctx context.Context, conn *grpc.ClientConn) error {
		var err error
		out, err = tenetv1.NewTenetWorkerClient(conn).ExecuteTool(ctx, toProtoExecuteToolRequest(req))
		return err
	})
	if out == nil {
		return worker.ExecuteToolResponse{}, err
	}
	return fromProtoExecuteToolResponse(out), err
}

func (c *WorkerClient) HealthCheck(ctx context.Context) (worker.HealthCheckResponse, error) {
	var out *tenetv1.HealthCheckResponse
	err := c.invokeWithWorker(ctx, c.opts.ControlTimeout, func(ctx context.Context, conn *grpc.ClientConn) error {
		var err error
		out, err = tenetv1.NewTenetWorkerClient(conn).HealthCheck(ctx, &tenetv1.HealthCheckRequest{})
		return err
	})
	if out == nil {
		return worker.HealthCheckResponse{}, err
	}
	return worker.HealthCheckResponse{Status: out.Status, WorkerCount: int(out.WorkerCount), UptimeSeconds: out.UptimeSeconds}, err
}

func (c *WorkerClient) invokeWithWorker(ctx context.Context, timeout time.Duration, call func(context.Context, *grpc.ClientConn) error) error {
	address := c.direct
	var lease *WorkerLease
	if address == "" {
		var err error
		lease, err = c.opts.Registry.Lease()
		if err != nil {
			return err
		}
		defer lease.Release()
		address = lease.Worker().Address
	}
	return c.invokeWithRetry(ctx, address, timeout, call)
}

func (c *WorkerClient) invokeWithRetry(ctx context.Context, address string, timeout time.Duration, call func(context.Context, *grpc.ClientConn) error) error {
	var last error
	for attempt := 1; attempt <= c.opts.RetryMaxAttempts; attempt++ {
		if err := c.breaker.Allow(); err != nil {
			return err
		}
		callCtx, cancel := context.WithTimeout(ctx, timeout)
		conn, connErr := c.conn(address)
		err := connErr
		if err == nil {
			err = call(callCtx, conn)
		}
		cancel()
		if err == nil {
			c.breaker.RecordSuccess()
			return nil
		}
		last = err
		if !isRetryable(err) || attempt == c.opts.RetryMaxAttempts {
			c.breaker.RecordFailure()
			return err
		}
		c.breaker.RecordFailure()
		backoff := c.opts.RetryBackoffBase * time.Duration(1<<(attempt-1))
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return last
}

func (c *WorkerClient) conn(address string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn := c.conns[address]; conn != nil {
		return conn, nil
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[address] = conn
	return conn, nil
}

func isRetryable(err error) bool {
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.ResourceExhausted
}
