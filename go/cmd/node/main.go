package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/JeroZp/p2p-lab/internal/common"
	"github.com/JeroZp/p2p-lab/internal/core"
	"github.com/JeroZp/p2p-lab/internal/gossip"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	addr			:= flag.String("addr", "127.0.0.1:7000", "address yo listen on")
	dial			:= flag.String("dial", "", "address of peer to connect to (optional)")
	keyPath 		:= flag.String("key", "node_data/node.key", "path to Ed25519 key file")
	metricsAddr		:= flag.String("metrics", "127.0.0.1:9100", "address for /metrics endpoint")
	otelEndpoint	:= flag.String("otel", "", "OpenTelemetry collector endpoint (e.g localhost:4318)")
	flag.Parse()

	node, err := core.NewNode(*keyPath)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	// Initialize tracing if endpoint provided
	if *otelEndpoint != "" {
		shutdown, err := common.InitTracing(context.Background(), node.ID.String()[:8], *otelEndpoint)
		if err != nil {
			log.Printf("tracing unavailable: %v", err)
		} else {
			defer shutdown(context.Background())
			log.Printf("tracing enabled -> %s", *otelEndpoint)
		}
	}

	if err := node.Listen(*addr); err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("node %s listening on %s", node.ID, *addr)

	engine := gossip.NewEngine(node)
	engine.Start()

	if *dial != "" {
		if err := node.Dial(*dial); err != nil {
			log.Fatalf("dial %s: %v", *dial, err)
		}
		log.Printf("connected to %s", *dial)
	}

	gatherers := prometheus.Gatherers{
		node.Registry(),
		engine.Registry(),
	}
	// expose Prometheus metrics
	http.Handle("/metrics", promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{}))
	go func ()  {
		log.Printf("metrics available at https://%s/metrics", *metricsAddr)
		if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
			log.Printf("metrics serve error: %v", err)
		}
	}()

	// Print incoming gossip to stdout
	go func ()  {
		for data := range engine.Received {
			fmt.Printf("[gossip] %s\n", data)
		}
	}()

	// Print incoming messages to stdout
	go func ()  {
		for msg := range node.Inbound {
			fmt.Printf("[%s] %s -> %s\n", msg.Type, msg.From, node.ID)
		}
	}()

	// block until Ctrl+C or SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	engine.Stop()
	node.Shutdown()
}
