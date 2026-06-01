package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Akiyoshi02/Raft_KV_Store/internal/kvstore"
	"github.com/Akiyoshi02/Raft_KV_Store/internal/raft"
	"github.com/Akiyoshi02/Raft_KV_Store/internal/server"
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
		for _, peer := range strings.Split(*peersStr, ",") {
			if peer = strings.TrimSpace(peer); peer != "" {
				peers = append(peers, peer)
			}
		}
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

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.Start()
	}()

	slog.Info("node is running", "id", *nodeID, "address", *address, "peers", peers)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	exitCode := 0
	select {
	case <-quit:
		slog.Info("shutting down gracefully...")
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server stopped unexpectedly", "reason", err)
			exitCode = 1
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Stop(ctx); err != nil {
		slog.Error("failed to stop HTTP server gracefully", "error", err)
		exitCode = 1
	}
	node.Stop()
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}
