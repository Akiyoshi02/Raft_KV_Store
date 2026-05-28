package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Akiyoshi02/raft-kv-store/internal/kvstore"
	"github.com/Akiyoshi02/raft-kv-store/internal/raft"
	"github.com/Akiyoshi02/raft-kv-store/internal/server"
)

func main() {
	nodeID := flag.String("id", "", "Unique node ID (e.g. node1)")
	address := flag.String("addr", "", "Address to listen on (e.g. localhost:8001)")
	peersStr := flag.String("peers", "", "Comma-separated addresses of other nodes")
	dataDir := flag.String("data", "", "Directory for persistent storage")
	flag.Parse()

	if *nodeID == "" || *address == "" || *dataDir == "" {
		slog.Error("flags -id, -addr, and -data are all required")
		os.Exit(1)
	}

	var peers []string
	if *peersStr != "" {
		peers = strings.Split(*peersStr, ",")
	}

	cfg := raft.Config{
		NodeID:  *nodeID,
		Address: *address,
		Peers:   peers,
		DataDir: *dataDir,
	}

	store := kvstore.New()

	node, err := raft.NewNode(cfg, store)
	if err != nil {
		slog.Error("failed to create raft node", "error", err)
		os.Exit(1)
	}

	srv := server.New(node, *address)

	node.Start()

	// Run the HTTP server in the background
	go func() {
		if err := srv.Start(); err != nil {
			slog.Info("HTTP server stopped", "reason", err)
		}
	}()

	slog.Info("node is running", "id", *nodeID, "address", *address, "peers", peers)

	// Block until Ctrl+C or SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Stop(ctx)
	node.Stop()
}
