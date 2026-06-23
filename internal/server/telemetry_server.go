package server

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/ahan-halder/fleet-telemetry/internal/anomaly"
	"github.com/ahan-halder/fleet-telemetry/internal/bigtable"
	"github.com/ahan-halder/fleet-telemetry/internal/pubsub"
	telemetryv1 "github.com/ahan-halder/fleet-telemetry/gen/go/fleet/telemetry/v1"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Deduplicator prevents processing the same frame twice.
type Deduplicator struct {
	mu   sync.RWMutex
	seen map[string]time.Time
}

func NewDeduplicator() *Deduplicator {
	return &Deduplicator{
		seen: make(map[string]time.Time),
	}
}

func (d *Deduplicator) Seen(ctx context.Context, key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.seen[key]; exists {
		return true
	}
	d.seen[key] = time.Now()
	// Cleanup should be done periodically in a real implementation
	return false
}

// TelemetryServer implements the TelemetryService.
type TelemetryServer struct {
	telemetryv1.UnimplementedTelemetryServiceServer

	bigtable        *bigtable.Client
	pubsub          *pubsub.Publisher
	anomalyDetector anomaly.Detector
	deduplicator    *Deduplicator
	
	// Channels for alerting subscriptions
	alertSubs muSub
}

type muSub struct {
	sync.RWMutex
	subs map[string]chan *telemetryv1.AlertNotification
}

func NewTelemetryServer(bt *bigtable.Client, ps *pubsub.Publisher, detector anomaly.Detector) *TelemetryServer {
	return &TelemetryServer{
		bigtable:        bt,
		pubsub:          ps,
		anomalyDetector: detector,
		deduplicator:    NewDeduplicator(),
		alertSubs: muSub{
			subs: make(map[string]chan *telemetryv1.AlertNotification),
		},
	}
}

func (s *TelemetryServer) StreamTelemetry(stream telemetryv1.TelemetryService_StreamTelemetryServer) error {
	ctx := stream.Context()

	for {
		// Recv blocks until a MetricFrame arrives or the stream closes.
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil // Agent closed the stream gracefully.
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv error: %v", err)
		}

		if frame.AgentId == "" || frame.FleetId == "" {
			return status.Errorf(codes.InvalidArgument, "agent_id and fleet_id are required")
		}

		// Deduplicate idempotent frames before any side effect.
		if s.deduplicator.Seen(ctx, frame.IdempotencyKey) {
			_ = stream.Send(&telemetryv1.StreamResponse{
				Response: &telemetryv1.StreamResponse_Ack{
					Ack: &telemetryv1.StreamAck{
						IdempotencyKey: frame.IdempotencyKey,
					},
				},
			})
			continue
		}

		// Fan out: Bigtable write + Pub/Sub publish happen concurrently.
		g, gctx := errgroup.WithContext(ctx)

		g.Go(func() error {
			if s.bigtable == nil {
				return nil // For local mode testing
			}
			return s.bigtable.WriteMetricFrame(gctx, frame)
		})

		g.Go(func() error {
			if s.anomalyDetector.IsAnomaly(frame) {
				if s.pubsub != nil {
					return s.pubsub.PublishAnomaly(gctx, frame)
				}
			}
			return nil
		})

		if err := g.Wait(); err != nil {
			// Return UNAVAILABLE — clients should back off and retry.
			return bigtableWriteErr(err, 500*time.Millisecond)
		}

		// Acknowledge the frame.
		if sendErr := stream.Send(&telemetryv1.StreamResponse{
			Response: &telemetryv1.StreamResponse_Ack{
				Ack: &telemetryv1.StreamAck{
					IdempotencyKey:   frame.IdempotencyKey,
					ServerReceivedAt: timestamppb.Now(),
				},
			},
		}); sendErr != nil {
			return sendErr
		}
	}
}

func (s *TelemetryServer) SubscribeAlerts(req *telemetryv1.AlertSubscribeRequest, stream telemetryv1.TelemetryService_SubscribeAlertsServer) error {
	if req.AgentId == "" {
		return status.Errorf(codes.InvalidArgument, "agent_id is required")
	}

	ch := make(chan *telemetryv1.AlertNotification, 10)
	s.alertSubs.Lock()
	s.alertSubs.subs[req.AgentId] = ch
	s.alertSubs.Unlock()

	defer func() {
		s.alertSubs.Lock()
		delete(s.alertSubs.subs, req.AgentId)
		s.alertSubs.Unlock()
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case alert := <-ch:
			if err := stream.Send(alert); err != nil {
				return err
			}
		}
	}
}

func (s *TelemetryServer) GetFleetStatus(ctx context.Context, req *telemetryv1.GetFleetStatusRequest) (*telemetryv1.GetFleetStatusResponse, error) {
	// Dummy implementation for now
	return &telemetryv1.GetFleetStatusResponse{
		FleetId:          req.FleetId,
		ActiveAgentCount: 1,
		Agents: []*telemetryv1.AgentStatusSummary{
			{
				AgentId:                "dummy-agent",
				AvgCpuUtilizationPct:   50.0,
				LastSeenUnixMs:         uint64(time.Now().UnixMilli()),
				Status:                 telemetryv1.AlertNotification_SEVERITY_INFO,
			},
		},
		AsOf: timestamppb.Now(),
	}, nil
}
