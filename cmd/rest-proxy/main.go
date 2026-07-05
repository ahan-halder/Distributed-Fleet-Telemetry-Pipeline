package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	telemetryv1 "github.com/ahan-halder/fleet-telemetry/gen/go/fleet/telemetry/v1"
)

func main() {
	gatewayAddr := flag.String("gateway-endpoint", "localhost:50051", "endpoint of the gRPC service")
	port := flag.String("port", "8080", "The server port")
	flag.Parse()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	
	log.Printf("Dialing gRPC server at %s", *gatewayAddr)
	err := telemetryv1.RegisterTelemetryServiceHandlerFromEndpoint(ctx, mux, *gatewayAddr, opts)
	if err != nil {
		log.Fatalf("failed to start HTTP gateway: %v", err)
	}

	srv := &http.Server{
		Addr:    ":" + *port,
		Handler: mux,
	}

	go func() {
		log.Printf("Starting HTTP/REST proxy on port %s", *port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("failed to serve HTTP: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Println("Shutting down REST proxy...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("REST proxy shutdown error: %v", err)
	} else {
		log.Println("REST proxy stopped gracefully")
	}
}
