package transfer

import "github.com/prometheus/client_golang/prometheus"

type transferMetrics struct {
	registry 		*prometheus.Registry
	chunkDuration	prometheus.Histogram
	bytesTotal		prometheus.Counter
	verifyFailures	prometheus.Counter
}

func newTransferMetrics() *transferMetrics {
	reg := prometheus.NewRegistry()

	chunkDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: 	 "p2p_transfer_chunk_duration_seconds",
		Help: 	 "Per-chunk fecth latency in seconds",
		Buckets: prometheus.DefBuckets,
	})
	bytesTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name:	"p2p_transfer_bytes_total",
		Help:	"Total bytes receive from peers",
	})
	verifyFailures := prometheus.NewCounter(prometheus.CounterOpts{
		Name:	"p2p_transfer_verification_failures_total",
		Help:	"Chunk rejected because they hash did not match.",
	})

	reg.MustRegister(chunkDuration, bytesTotal, verifyFailures)

	return &transferMetrics{
		registry: 		reg,
		chunkDuration: chunkDuration,
		bytesTotal: 	bytesTotal,
		verifyFailures: verifyFailures,
	}
}