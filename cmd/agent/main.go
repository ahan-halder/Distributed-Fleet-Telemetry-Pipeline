package main

import (
	"context"
	"flag"
	"log"
	"time"
	
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	telemetryv1 "github.com/ahan-halder/fleet-telemetry/gen/go/fleet/telemetry/v1"
)

func main() {
	gatewayAddr := flag.String("gateway", "localhost:50051", "Gateway address")
	agentID := flag.String("agent-id", "agent-local-001", "Agent ID")
	fleetID := flag.String("fleet-id", "fleet-dev", "Fleet ID")
	flag.Parse()

	// Connect to gateway
	conn, err := grpc.NewClient(*gatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := telemetryv1.NewTelemetryServiceClient(conn)

	// Open bidirectional stream
	ctx := context.Background()
	stream, err := c.StreamTelemetry(ctx)
	if err != nil {
		log.Fatalf("could not start stream: %v", err)
	}

	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				log.Fatalf("error receiving: %v", err)
			}
			switch t := res.Response.(type) {
			case *telemetryv1.StreamResponse_Ack:
				log.Printf("Received ack for %s", t.Ack.IdempotencyKey)
			case *telemetryv1.StreamResponse_Alert:
				log.Printf("Received alert! %s: %s", t.Alert.Severity, t.Alert.Message)
			}
		}
	}()

	// Simulate sending frames
	for {
		cpu := 40.0 + (float64(time.Now().UnixNano()%2000) / 100.0) // 40-60%
		// Occasional anomaly
		if time.Now().Unix()%10 == 0 {
			cpu = 95.0
		}

		frame := &telemetryv1.MetricFrame{
			AgentId:        *agentID,
			FleetId:        *fleetID,
			CollectedAt:    timestamppb.Now(),
			IdempotencyKey: uuid.New().String(),
			System: &telemetryv1.SystemMetrics{
				CpuUtilizationPct: cpu,
				MemoryUsedBytes:   uint64(2048 + time.Now().UnixNano()%1024),
				MemoryTotalBytes:  8192,
			},
		}

		if err := stream.Send(frame); err != nil {
			log.Fatalf("error sending frame: %v", err)
		}
		
		time.Sleep(1 * time.Second)
	}
}
