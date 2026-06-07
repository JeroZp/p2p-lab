package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/JeroZp/p2p-lab/internal/core"
)

func main() {
	addr	:= flag.String("addr", "127.0.0.1:7000", "address yo listen on")
	dial	:= flag.String("dial", "", "address of peer to connect to (optional)")
	keyPath := flag.String("key", "node_data/node.key", "path to Ed25519 key file")
	flag.Parse()

	node, err := core.NewNode(*keyPath)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	if err := node.Listen(*addr); err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("node %s listening on %s", node.ID, *addr)

	if *dial != "" {
		if err := node.Dial(*dial); err != nil {
			log.Fatalf("dial %s: %v", *dial, err)
		}
		log.Printf("connected to %s", *dial)
	}

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
	node.Shutdown()
}
