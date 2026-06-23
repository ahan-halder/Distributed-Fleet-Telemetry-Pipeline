package server

import (
	fleetauth "github.com/ahan-halder/fleet-telemetry/internal/auth"
	"github.com/ahan-halder/fleet-telemetry/internal/observability"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type Config struct {
	// Add config fields if needed.
}

// NewServer creates a new gRPC server with the complete interceptor chain.
func NewServer(cfg *Config) (*grpc.Server, *health.Server) {
	metrics := observability.NewServerMetrics()

	panicRecovery := func(p any) (err error) {
		// Emit a log + span event before returning Internal.
		zap.L().Error("panic recovered in gRPC handler",
			zap.Any("panic_value", p),
			zap.Stack("stack"),
		)
		return status.Errorf(codes.Internal, "internal server error")
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()), // OTel via StatsHandler (preferred over interceptor).
		grpc.ChainUnaryInterceptor(
			metrics.UnaryServerInterceptor(),
			logging.UnaryServerInterceptor(observability.InterceptorLogger()),
			auth.UnaryServerInterceptor(fleetauth.JWTAuthFunc),
			recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(panicRecovery)),
		),
		grpc.ChainStreamInterceptor(
			metrics.StreamServerInterceptor(),
			logging.StreamServerInterceptor(observability.InterceptorLogger()),
			auth.StreamServerInterceptor(fleetauth.JWTAuthFunc),
			recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(panicRecovery)),
		),
	)

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	return grpcServer, healthServer
}
