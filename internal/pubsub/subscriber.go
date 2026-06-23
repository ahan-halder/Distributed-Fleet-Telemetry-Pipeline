package pubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	telemetryv1 "github.com/ahan-halder/fleet-telemetry/gen/go/fleet/telemetry/v1"
	"google.golang.org/protobuf/proto"
)

// Subscriber wraps a Pub/Sub subscription for alert push-backs.
type Subscriber struct {
	sub *pubsub.Subscription
}

// NewSubscriber creates a new Subscriber.
func NewSubscriber(client *pubsub.Client, subID string) *Subscriber {
	return &Subscriber{
		sub: client.Subscription(subID),
	}
}

// ReceiveAlerts receives alerts and pushes them to a channel.
func (s *Subscriber) ReceiveAlerts(ctx context.Context, out chan<- *telemetryv1.AlertNotification) error {
	return s.sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var alert telemetryv1.AlertNotification
		if err := proto.Unmarshal(msg.Data, &alert); err != nil {
			msg.Nack()
			return
		}
		
		out <- &alert
		msg.Ack()
	})
}
