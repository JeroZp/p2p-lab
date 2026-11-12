package dht

import (
	"github.com/prometheus/client_golang/prometheus"
)

type dhtMetrics struct {
	registry           *prometheus.Registry
	lookupDuration     prometheus.Histogram
	lookupHops         prometheus.Histogram
	routingTableSize   prometheus.Gauge
}

func newDHTMetrics() *dhtMetrics {
	reg := prometheus.NewRegistry()

	lookupDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "p2p_dht_lookup_duration_seconds",
		Help:    "End-to-end DHT lookup latency in seconds.",
		Buckets: prometheus.DefBuckets,
	})
	lookupHops := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "p2p_dht_lookup_hops",
		Help:    "Number of hops per DHT lookup.",
		Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})
	routingTableSize := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "p2p_dht_routing_table_size",
		Help: "Number of entries in the routing table.",
	})

	reg.MustRegister(lookupDuration, lookupHops, routingTableSize)

	return &dhtMetrics{
		registry:         reg,
		lookupDuration:   lookupDuration,
		lookupHops:       lookupHops,
		routingTableSize: routingTableSize,
	}
}