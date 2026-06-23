package pubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	telemetryv1 "github.com/ahan-halder/fleet-telemetry/gen/go/fleet/telemetry/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"
)

// Publisher wraps a Pub/Sub topic for anomaly events.
type Publisher struct {
	topic *pubsub.Topic
}

// NewPublisher creates a new Publisher.
func NewPublisher(client *pubsub.Client, topicID string) *Publisher {
	return &Publisher{
		topic: client.Topic(topicID),
	}
}

// PublishAnomaly publishes a metric frame as an anomaly.
func (p *Publisher) PublishAnomaly(ctx context.Context, frame *telemetryv1.MetricFrame) error {
	// Extract W3C traceparent + tracestate from the current context.
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	data, err := proto.Marshal(frame) // Usually we'd map this to a specific Anomaly message
	if err != nil {
		return err
	}

	msg := &pubsub.Message{
		Data: data,
		// Embed the trace context in message attributes
		Attributes: map[string]string{
			"traceparent": carrier["traceparent"],
			"tracestate":  carrier["tracestate"],
			"agent_id":    frame.AgentId,
			"fleet_id":    frame.FleetId,
		},
	}

	_, err = p.topic.Publish(ctx, msg).Get(ctx)
	return err
}

// Stop stops the publisher.
func (p *Publisher) Stop() {
	p.topic.Stop()
}
