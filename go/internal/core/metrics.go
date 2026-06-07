package core

import (
	"github.com/prometheus/client_golang/prometheus"
)

type coreMetrics struct {
	registry              *prometheus.Registry
	connectionsActive     prometheus.Gauge
	messagesSentTotal     *prometheus.CounterVec
	messagesReceivedTotal *prometheus.CounterVec
}

func newCoreMetrics() *coreMetrics {
	reg := prometheus.NewRegistry()

	connectionsActive := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "p2p_connections_active",
		Help: "Current number of active TCP connections.",
	})
	messagesSentTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "p2p_messages_sent_total",
		Help: "Total messages sent, labelled by type.",
	}, []string{"type"})
	messagesReceivedTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "p2p_messages_received_total",
		Help: "Total messages received, labelled by type.",
	}, []string{"type"})

	reg.MustRegister(connectionsActive, messagesSentTotal, messagesReceivedTotal)

	return &coreMetrics{
		registry:              reg,
		connectionsActive:     connectionsActive,
		messagesSentTotal:     messagesSentTotal,
		messagesReceivedTotal: messagesReceivedTotal,
	}
}