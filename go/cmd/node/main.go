package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/JeroZp/p2p-lab/internal/common"
	"github.com/JeroZp/p2p-lab/internal/core"
	"github.com/JeroZp/p2p-lab/internal/dht"
	"github.com/JeroZp/p2p-lab/internal/gossip"
	"github.com/JeroZp/p2p-lab/internal/transfer"
)

const maxUploadSize = 100 << 20 // 100 MB

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "P2P address to listen on")
	dialList := flag.String("dial", "", "comma-separated peer addresses to connect to")
	keyPath := flag.String("key", "node_data/node.key", "path to Ed25519 key file")
	httpAddr := flag.String("metrics", "127.0.0.1:9100", "HTTP address for /metrics and the API")
	otelEndpoint := flag.String("otel", "", "OpenTelemetry collector endpoint (e.g. localhost:4318)")
	flag.Parse()

	// --- Identity and tracing ---
	node, err := core.NewNode(*keyPath)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	if *otelEndpoint != "" {
		shutdown, err := common.InitTracing(context.Background(), node.ID.String()[:8], *otelEndpoint)
		if err != nil {
			log.Printf("tracing unavailable: %v", err)
		} else {
			defer shutdown(context.Background())
			log.Printf("tracing enabled → %s", *otelEndpoint)
		}
	}

	if err := node.Listen(*addr); err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("node %s listening on %s", node.ID, *addr)

	// --- Subsystems: created BEFORE dialing so their subscriptions
	// exist when the first messages arrive ---
	gossipEngine := gossip.NewEngine(node)
	gossipEngine.Start()

	dhtNode := dht.NewDHTNode(node, *addr)
	dhtNode.Start()

	transferEngine := transfer.NewEngine(node, dhtNode, *addr)
	transferEngine.Start()

	// --- Connect to peers ---
	if *dialList != "" {
		for _, peer := range strings.Split(*dialList, ",") {
			peer = strings.TrimSpace(peer)
			if err := dialWithRetry(node, peer); err != nil {
				log.Printf("could not connect to %s: %v", peer, err)
				continue
			}
			log.Printf("connected to %s", peer)
		}
	}

	go bootstrapDHT(node, dhtNode)

	// --- HTTP: metrics + API ---
	gatherers := prometheus.Gatherers{
		node.Registry(),
		gossipEngine.Registry(),
		dhtNode.Registry(),
		transferEngine.Registry(),
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{}))
	mux.HandleFunc("POST /share", shareHandler(transferEngine))
	mux.HandleFunc("GET /fetch/{root}", fetchHandler(transferEngine))

	go func() {
		log.Printf("HTTP (metrics + API) on http://%s", *httpAddr)
		if err := http.ListenAndServe(*httpAddr, mux); err != nil {
			log.Printf("http server error: %v", err)
		}
	}()

	go func() {
		for data := range gossipEngine.Received {
			fmt.Printf("[gossip] %s\n", data)
		}
	}()

	// --- Wait for shutdown ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	transferEngine.Stop()
	dhtNode.Stop()
	gossipEngine.Stop()
	node.Shutdown()
}

// dialWithRetry retries a dial a few times. In Docker, depends_on only
// waits for the container to START, not for the peer to be listening.
func dialWithRetry(node *core.Node, addr string) error {
	var err error
	for attempt := 1; attempt <= 10; attempt++ {
		if err = node.Dial(addr); err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return err
}

// bootstrapDHT keeps the routing table in sync with the connected peers.
// Peers that connect later (after this node started) are picked up on
// the next tick. Our DHT sends RPCs by NodeID, so the ID is enough.
func bootstrapDHT(node *core.Node, d *dht.Node) {
	time.Sleep(2 * time.Second)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	first := true
	for {
		for _, id := range node.Peers() {
			d.AddNode(dht.Entry{ID: id})
		}
		if first {
			found := d.FindNode(node.ID)
			log.Printf("DHT bootstrap: %d peers connected, lookup found %d nodes",
				len(node.Peers()), len(found))
			first = false
		}
		<-ticker.C
	}
}

// shareHandler: POST /share with the file as the request body.
// Returns the Merkle root hash as hex.
func shareHandler(t *transfer.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tmp, err := os.CreateTemp("", "share-*")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer os.Remove(tmp.Name())

		size, err := io.Copy(tmp, http.MaxBytesReader(w, r.Body, maxUploadSize))
		tmp.Close()
		if err != nil {
			http.Error(w, "upload failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		m, err := t.Share(tmp.Name())
		if err != nil {
			http.Error(w, "share failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		log.Printf("shared %d bytes in %d chunks, root %x", size, len(m.Chunks), m.Root)
		fmt.Fprintf(w, "%x\n", m.Root)
	}
}

// fetchHandler: GET /fetch/<root-hex>. Returns the file bytes.
func fetchHandler(t *transfer.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, err := hex.DecodeString(r.PathValue("root"))
		if err != nil || len(raw) != 32 {
			http.Error(w, "root must be 64 hex characters", http.StatusBadRequest)
			return
		}
		var root [32]byte
		copy(root[:], raw)

		start := time.Now()
		data, err := t.FetchFile(root)
		if err != nil {
			http.Error(w, "fetch failed: "+err.Error(), http.StatusBadGateway)
			return
		}

		log.Printf("fetched %d bytes in %v", len(data), time.Since(start))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(data)
	}
}