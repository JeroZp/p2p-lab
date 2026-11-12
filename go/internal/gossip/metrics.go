package gossip

import (
	"github.com/prometheus/client_golang/prometheus"
)

type gossipMetrics struct {
	registry		*prometheus.Registry
	forwardedTotal	prometheus.Counter
	duplicatesTotal prometheus.Counter
	deliveryLatency prometheus.Histogram
}

func newGossipMetrics() *gossipMetrics {
	reg := prometheus.NewRegistry()

	forwardedTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "p2p_gossip_forwarded_total",
		Help: "Total gossip messages forwarded to peers.",
	})
	duplicatesTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "p2p_gossip_duplicates_total",
		Help: "Total gossip messages dropped as duplicates.",
	})
	deliveryLatency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "p2p_gossip_delivery_seconds",
		Help:    "Time from broadcast at the originator to first delivery on this node",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	})

	reg.MustRegister(forwardedTotal, duplicatesTotal, deliveryLatency)

	return &gossipMetrics{
		registry:        reg,
		forwardedTotal:  forwardedTotal,
		duplicatesTotal: duplicatesTotal,
		deliveryLatency: deliveryLatency,
	}
}