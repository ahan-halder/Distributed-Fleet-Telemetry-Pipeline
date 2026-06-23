package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ahan-halder/fleet-telemetry/internal/anomaly"
	"github.com/ahan-halder/fleet-telemetry/internal/server"
	telemetryv1 "github.com/ahan-halder/fleet-telemetry/gen/go/fleet/telemetry/v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	port := flag.String("port", "50051", "The server port")
	mode := flag.String("mode", "local", "Execution mode (local or gcp)")
	flag.Parse()

	log.Printf("Starting gateway in %s mode on port %s", *mode, *port)

	detector := anomaly.NewSimpleThresholdDetector(90.0, 90.0)
	// For local mode, we pass nil for Bigtable and Pub/Sub
	telemetryServer := server.NewTelemetryServer(nil, nil, detector)

	grpcServer, healthServer := server.NewServer(&server.Config{})
	telemetryv1.RegisterTelemetryServiceServer(grpcServer, telemetryServer)
	
	// Mark the service as SERVING
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("fleet.telemetry.v1.TelemetryService", healthpb.HealthCheckResponse_SERVING)

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		log.Printf("server listening at %v", lis.Addr())
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	
	log.Println("Shutting down gateway...")
	// Create a timeout for graceful shutdown
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	t := time.NewTimer(10 * time.Second)
	select {
	case <-t.C:
		log.Println("Shutdown timeout exceeded, forcing stop")
		grpcServer.Stop()
	case <-stopped:
		t.Stop()
		log.Println("Gateway stopped gracefully")
	}
}
