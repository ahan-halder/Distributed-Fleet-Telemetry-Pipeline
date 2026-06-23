package server_test

import (
	"context"
	"io"
	"net"
	"testing"

	telemetryv1 "github.com/ahan-halder/fleet-telemetry/gen/go/fleet/telemetry/v1"
	"github.com/ahan-halder/fleet-telemetry/internal/anomaly"
	"github.com/ahan-halder/fleet-telemetry/internal/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const bufSize = 1024 * 1024

func setupBufconnServer(t *testing.T) (*grpc.ClientConn, func()) {
	lis := bufconn.Listen(bufSize)
	grpcServer, _ := server.NewServer(&server.Config{})
	
	// Create telemetry server with nil sinks to test pure streaming logic
	detector := anomaly.NewSimpleThresholdDetector(90.0, 90.0)
	telemetryServer := server.NewTelemetryServer(nil, nil, detector)
	
	telemetryv1.RegisterTelemetryServiceServer(grpcServer, telemetryServer)
	
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			panic(err) // Serve only returns errors for test failures in bufconn
		}
	}()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	return conn, func() {
		conn.Close()
		grpcServer.Stop()
		lis.Close()
	}
}

func TestStreamTelemetry_Ack(t *testing.T) {
	conn, cleanup := setupBufconnServer(t)
	defer cleanup()

	client := telemetryv1.NewTelemetryServiceClient(conn)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "bearer test-token")
	stream, err := client.StreamTelemetry(ctx)
	if err != nil {
		t.Fatalf("StreamTelemetry failed: %v", err)
	}

	idempotencyKey := "test-uuid-1234"
	frame := &telemetryv1.MetricFrame{
		AgentId:        "agent-1",
		FleetId:        "fleet-1",
		CollectedAt:    timestamppb.Now(),
		IdempotencyKey: idempotencyKey,
		System: &telemetryv1.SystemMetrics{
			CpuUtilizationPct: 50.0,
		},
	}

	if err := stream.Send(frame); err != nil {
		t.Fatalf("Failed to send frame: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Failed to receive response: %v", err)
	}

	ack, ok := resp.Response.(*telemetryv1.StreamResponse_Ack)
	if !ok {
		t.Fatalf("Expected ack, got: %v", resp)
	}

	if ack.Ack.IdempotencyKey != idempotencyKey {
		t.Errorf("Expected idempotency key %s, got %s", idempotencyKey, ack.Ack.IdempotencyKey)
	}

	// Close stream gracefully
	stream.CloseSend()
	_, err = stream.Recv()
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}
