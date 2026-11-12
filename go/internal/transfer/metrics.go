package transfer

import "github.com/prometheus/client_golang/prometheus"

type transferMetrics struct {
	registry 		*prometheus.Registry
	chunkDuration	prometheus.Histogram
	bytesTotal		prometheus.Counter
	verifyFailures	prometheus.Counter
	fetchDuration 	prometheus.Histogram
	fetchesTotal	prometheus.CounterVec
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
	fetchDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: 	 "p2p_transfer_fetch_duration_seconds",
		Help: 	 "Duration of succesful FetchFile calls, excluding HTTP delivery",
		// 5 must be a bucket boundry
		Buckets: []float64{0.25, 0.5, 1, 2, 5, 10, 30},
	})
	fetchesTotal := *prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:	"p2p_transfer_fetches_total",
		Help:	"FetchFile calls, labelled by result (ok or error)",
	}, []string{"status"})

	reg.MustRegister(chunkDuration, bytesTotal, verifyFailures, fetchDuration, fetchesTotal)

	// make both series visible before the first fetch
	fetchesTotal.WithLabelValues("ok").Add(0)
	fetchesTotal.WithLabelValues("error").Add(0)

	return &transferMetrics{
		registry: 		reg,
		chunkDuration: chunkDuration,
		bytesTotal: 	bytesTotal,
		verifyFailures: verifyFailures,
		fetchDuration: fetchDuration,
		fetchesTotal: 	fetchesTotal,
	}
}