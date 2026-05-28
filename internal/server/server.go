package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Akiyoshi02/raft-kv-store/internal/raft"
)

// Server exposes both the Raft RPC endpoints (for inter-node communication)
// and the public client API (for reading and writing data).
type Server struct {
	node       *raft.Node
	httpServer *http.Server
}

// New creates a Server wired to the given Raft node.
func New(node *raft.Node, address string) *Server {
	s := &Server{node: node}

	mux := http.NewServeMux()

	// ── Raft RPC routes (node-to-node only) ──────────────────────────────────
	mux.HandleFunc("POST /raft/vote", s.handleRequestVote)
	mux.HandleFunc("POST /raft/append", s.handleAppendEntries)

	// ── Client API routes ─────────────────────────────────────────────────────
	mux.HandleFunc("GET /api/keys/{key}", s.handleGet)
	mux.HandleFunc("PUT /api/keys/{key}", s.handleSet)
	mux.HandleFunc("DELETE /api/keys/{key}", s.handleDelete)
	mux.HandleFunc("GET /api/keys", s.handleGetAll)
	mux.HandleFunc("GET /api/status", s.handleStatus)

	s.httpServer = &http.Server{
		Addr:    address,
		Handler: mux,
	}

	return s
}

// Start begins listening. This call blocks until the server stops.
func (s *Server) Start() error {
	slog.Info("HTTP server listening", "address", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

// Stop gracefully shuts the server down.
func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// ── Raft RPC handlers ─────────────────────────────────────────────────────────

func (s *Server) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var args raft.RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reply := s.node.HandleRequestVote(args)
	writeJSON(w, http.StatusOK, reply)
}

func (s *Server) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var args raft.AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	reply := s.node.HandleAppendEntries(args)
	writeJSON(w, http.StatusOK, reply)
}

// ── Client API handlers ───────────────────────────────────────────────────────

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value, ok := s.node.GetValue(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, errorResponse{"key not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": value})
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if strings.Contains(key, " ") {
		writeJSON(w, http.StatusBadRequest, errorResponse{"key must not contain spaces"})
		return
	}

	var req struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{err.Error()})
		return
	}

	command := "SET " + key + " " + req.Value
	if err := s.node.Submit(command); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	command := "DELETE " + key
	if err := s.node.Submit(command); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleGetAll(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.node.GetAll())
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	state, term := s.node.GetState()
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":   s.node.GetNodeID(),
		"state":     state.String(),
		"term":      term,
		"leader_id": s.node.GetLeaderID(),
		"is_leader": s.node.IsLeader(),
	})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
