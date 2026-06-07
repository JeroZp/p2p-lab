package gossip

import (
	"github.com/prometheus/client_golang/prometheus"
)

type gossipMetrics struct {
	registry		*prometheus.Registry
	forwardedTotal	prometheus.Counter
	duplicatesTotal prometheus.Counter
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

	reg.MustRegister(forwardedTotal, duplicatesTotal)

	return &gossipMetrics{
		registry:        reg,
		forwardedTotal:  forwardedTotal,
		duplicatesTotal: duplicatesTotal,
	}
}