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

	"github.com/google/uuid"
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
	d := &Deduplicator{
		seen: make(map[string]time.Time),
	}
	// Start background cleanup
	go d.cleanupLoop()
	return d
}

func (d *Deduplicator) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	for range ticker.C {
		d.mu.Lock()
		now := time.Now()
		for k, v := range d.seen {
			if now.Sub(v) > 10*time.Minute {
				delete(d.seen, k)
			}
		}
		d.mu.Unlock()
	}
}

func (d *Deduplicator) Seen(ctx context.Context, key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.seen[key]; exists {
		return true
	}
	d.seen[key] = time.Now()
	return false
}

type agentState struct {
	cpu        float64
	lastSeenMs uint64
	status     telemetryv1.AlertNotification_Severity
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

	muFleet sync.RWMutex
	// fleet_id -> map[agent_id]agentState
	fleetAgents map[string]map[string]agentState
}

type muSub struct {
	sync.RWMutex
	subs map[string]chan *telemetryv1.AlertNotification
}

func NewTelemetryServer(bt *bigtable.Client, ps *pubsub.Publisher, detector anomaly.Detector) *TelemetryServer {
	s := &TelemetryServer{
		bigtable:        bt,
		pubsub:          ps,
		anomalyDetector: detector,
		deduplicator:    NewDeduplicator(),
		alertSubs: muSub{
			subs: make(map[string]chan *telemetryv1.AlertNotification),
		},
		fleetAgents: make(map[string]map[string]agentState),
	}
	go s.cleanupLoop()
	return s
}

func (s *TelemetryServer) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	for range ticker.C {
		s.muFleet.Lock()
		nowMs := uint64(time.Now().UnixMilli())
		for fleetID, agents := range s.fleetAgents {
			for agentID, state := range agents {
				// Evict if not seen for more than 1 hour (3600000 ms)
				if nowMs > state.lastSeenMs && (nowMs-state.lastSeenMs) > 3600000 {
					delete(agents, agentID)
				}
			}
			if len(agents) == 0 {
				delete(s.fleetAgents, fleetID)
			}
		}
		s.muFleet.Unlock()
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

		cpuVal := 0.0
		if frame.System != nil {
			cpuVal = frame.System.CpuUtilizationPct
		}
		currentSev := telemetryv1.AlertNotification_SEVERITY_INFO
		isAnom := s.anomalyDetector.IsAnomaly(frame)
		if isAnom {
			currentSev = telemetryv1.AlertNotification_SEVERITY_CRITICAL
		}

		// Track active agent state
		s.muFleet.Lock()
		if _, ok := s.fleetAgents[frame.FleetId]; !ok {
			s.fleetAgents[frame.FleetId] = make(map[string]agentState)
		}
		s.fleetAgents[frame.FleetId][frame.AgentId] = agentState{
			cpu:        cpuVal,
			lastSeenMs: uint64(frame.CollectedAt.AsTime().UnixMilli()),
			status:     currentSev,
		}
		s.muFleet.Unlock()

		// Fan out: Bigtable write + Pub/Sub publish happen concurrently.
		g, gctx := errgroup.WithContext(ctx)

		g.Go(func() error {
			if s.bigtable == nil {
				return nil // For local mode testing
			}
			return s.bigtable.WriteMetricFrame(gctx, frame)
		})

		g.Go(func() error {
			if isAnom {
				alert := &telemetryv1.AlertNotification{
					AlertId:     uuid.New().String(),
					AgentId:     frame.AgentId,
					FleetId:     frame.FleetId,
					Severity:    telemetryv1.AlertNotification_SEVERITY_CRITICAL,
					Message:     "Anomaly detected: metric threshold exceeded",
					TriggeredAt: timestamppb.Now(),
				}

				// Notify active SubscribeAlerts channels
				s.alertSubs.RLock()
				if ch, ok := s.alertSubs.subs[frame.AgentId]; ok {
					select {
					case ch <- alert:
					default:
					}
				}
				s.alertSubs.RUnlock()

				// Push back over bidirectional stream
				_ = stream.Send(&telemetryv1.StreamResponse{
					Response: &telemetryv1.StreamResponse_Alert{Alert: alert},
				})

				if s.pubsub != nil {
					return s.pubsub.PublishAnomaly(gctx, frame)
				}
			}
			return nil
		})

		if err := g.Wait(); err != nil {
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
	s.muFleet.RLock()
	agentsMap, exists := s.fleetAgents[req.FleetId]
	// Make a shallow copy under lock
	mapCopy := make(map[string]agentState, len(agentsMap))
	for k, v := range agentsMap {
		mapCopy[k] = v
	}
	s.muFleet.RUnlock()

	limit := int(req.LastNRows)
	if limit <= 0 {
		limit = 10
	}

	var summaries []*telemetryv1.AgentStatusSummary
	for agentID, state := range mapCopy {
		avgCPU := state.cpu
		lastSeen := state.lastSeenMs
		if s.bigtable != nil {
			if btCPU, btTime, err := s.bigtable.ReadRecentAgentSummary(ctx, agentID, limit); err == nil && btTime > 0 {
				avgCPU = btCPU
				lastSeen = btTime
			}
		}
		summaries = append(summaries, &telemetryv1.AgentStatusSummary{
			AgentId:              agentID,
			AvgCpuUtilizationPct: avgCPU,
			LastSeenUnixMs:       lastSeen,
			Status:               state.status,
		})
	}

	if !exists || len(summaries) == 0 {
		summaries = []*telemetryv1.AgentStatusSummary{
			{
				AgentId:              "no-active-agents",
				AvgCpuUtilizationPct: 0.0,
				LastSeenUnixMs:       uint64(time.Now().UnixMilli()),
				Status:               telemetryv1.AlertNotification_SEVERITY_INFO,
			},
		}
	}

	return &telemetryv1.GetFleetStatusResponse{
		FleetId:          req.FleetId,
		ActiveAgentCount: uint32(len(summaries)),
		Agents:           summaries,
		AsOf:             timestamppb.Now(),
	}, nil
}
