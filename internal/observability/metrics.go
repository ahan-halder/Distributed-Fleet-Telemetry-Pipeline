package observability

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ActiveStreams tracks currently open bidirectional streams
	ActiveStreams = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "fleet_active_streams",
		Help: "Currently open bidirectional streams",
	})

	// BigtableWriteDuration tracks Bigtable write latency
	BigtableWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "fleet_bigtable_write_duration_seconds",
		Help:    "Bigtable write latency",
		Buckets: prometheus.DefBuckets,
	})

	// PubSubPublishDuration tracks Pub/Sub publish latency
	PubSubPublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "fleet_pubsub_publish_duration_seconds",
		Help:    "Pub/Sub publish latency",
		Buckets: prometheus.DefBuckets,
	})
)

// NewServerMetrics returns a new grpcprom.ServerMetrics
func NewServerMetrics() *grpcprom.ServerMetrics {
	return grpcprom.NewServerMetrics(
		grpcprom.WithServerHandlingTimeHistogram(
			grpcprom.WithHistogramBuckets([]float64{
				.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5,
			}),
		),
	)
}
