package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	tenetv1 "github.com/tenet/orchestrator/internal/gateway/gen/tenet/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

type OrchestratorServer struct {
	tenetv1.UnimplementedTenetOrchestratorServer
	id       string
	registry *WorkerRegistry
	server   *grpc.Server
}

func NewOrchestratorServer(orchestratorID string, registry *WorkerRegistry) *OrchestratorServer {
	if orchestratorID == "" {
		orchestratorID = "tenet-orchestrator"
	}
	if registry == nil {
		registry = NewWorkerRegistry()
	}
	srv := &OrchestratorServer{id: orchestratorID, registry: registry}
	srv.server = grpc.NewServer()
	tenetv1.RegisterTenetOrchestratorServer(srv.server, srv)
	return srv
}

func (s *OrchestratorServer) Registry() *WorkerRegistry {
	return s.registry
}

func (s *OrchestratorServer) Serve(listener net.Listener) error {
	if listener == nil {
		return errors.New("listener is required")
	}
	return s.server.Serve(listener)
}

func (s *OrchestratorServer) Stop() {
	s.server.Stop()
}

func (s *OrchestratorServer) RegisterAgent(ctx context.Context, req *tenetv1.RegisterAgentRequest) (*tenetv1.RegisterAgentResponse, error) {
	host := "127.0.0.1"
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		addr := p.Addr.String()
		if h, _, err := net.SplitHostPort(addr); err == nil {
			host = strings.Trim(h, "[]")
		}
	}
	worker, err := s.registry.Register(req.AgentId, int(req.ListenPort), int(req.MaxConcurrency), host)
	if err != nil {
		return &tenetv1.RegisterAgentResponse{Accepted: false, OrchestratorId: s.id, Message: err.Error()}, err
	}
	return &tenetv1.RegisterAgentResponse{
		Accepted:       true,
		OrchestratorId: s.id,
		Message:        fmt.Sprintf("registered %s at %s", worker.AgentID, worker.Address),
	}, nil
}
