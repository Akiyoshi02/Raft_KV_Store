package raft

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Akiyoshi02/raft-kv-store/internal/kvstore"
)

// persistentState holds fields that must survive a crash.
// These are saved to disk before responding to any RPC.
type persistentState struct {
	CurrentTerm int    `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

// Node is a single member of the Raft cluster.
type Node struct {
	mu sync.Mutex

	config Config

	// Persistent state - must be saved to disk before responding to RPCs
	currentTerm int
	votedFor    string // NodeID we voted for this term, "" if none

	// Volatile state on all nodes
	commitIndex int       // highest log entry known to be committed
	lastApplied int       // highest log entry applied to the KV store
	state       NodeState // current role: Follower, Candidate, or Leader
	leaderID    string    // who we believe the current leader is

	// Volatile state on leaders only (reset after each election)
	nextIndex  map[string]int // for each peer: next log index to send
	matchIndex map[string]int // for each peer: highest index known to be replicated

	// Core components
	raftLog    *Log
	store      *kvstore.Store
	httpClient *http.Client

	// Timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Lifecycle
	stopCh chan struct{}
}

// NewNode creates a Raft node. It loads any existing persistent state
// from disk so the node can recover correctly after a crash.
func NewNode(config Config, store *kvstore.Store) (*Node, error) {
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	logPath := filepath.Join(config.DataDir, "raft.log")
	raftLog, err := NewLog(logPath)
	if err != nil {
		return nil, fmt.Errorf("create raft log: %w", err)
	}

	n := &Node{
		config:     config,
		state:      Follower,
		raftLog:    raftLog,
		store:      store,
		httpClient: &http.Client{Timeout: 50 * time.Millisecond},
		stopCh:     make(chan struct{}),
	}

	if err := n.loadPersistentState(); err != nil {
		raftLog.Close()
		return nil, fmt.Errorf("load persistent state: %w", err)
	}

	return n, nil
}

// Start begins the node's operation by arming the election timer.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()
	slog.Info("node started", "id", n.config.NodeID, "address", n.config.Address)
}

// Stop shuts the node down cleanly.
func (n *Node) Stop() {
	close(n.stopCh)
	n.mu.Lock()
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	n.mu.Unlock()
	n.raftLog.Close()
	slog.Info("node stopped", "id", n.config.NodeID)
}

// Submit accepts a write command from a client.
// Only the leader can accept writes — returns an error if this node is not the leader.
func (n *Node) Submit(command string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return fmt.Errorf("not leader: current leader is %q", n.leaderID)
	}

	entry := LogEntry{
		Index:   n.raftLog.LastIndex() + 1,
		Term:    n.currentTerm,
		Command: command,
	}
	if err := n.raftLog.Append(entry); err != nil {
		return fmt.Errorf("append to log: %w", err)
	}

	slog.Info("submitted entry", "index", entry.Index, "command", command)
	go n.replicateToAll()
	return nil
}

// GetState returns the node's current state and term. Safe for concurrent use.
func (n *Node) GetState() (NodeState, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.currentTerm
}

// GetLeaderID returns the NodeID of the current leader, or "" if unknown.
func (n *Node) GetLeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// IsLeader returns true if this node currently believes itself to be the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// GetValue reads a value from the KV store directly.
// In a production system, reads on followers can return stale data,
// but for our purposes this is fine.
func (n *Node) GetValue(key string) (string, bool) {
	return n.store.Get(key)
}

// ── Internal state transitions ────────────────────────────────────────────────

// becomeFollower steps the node down to follower.
// votedFor is only cleared when the term increases — within the same term
// we must remember who we voted for.
// Must be called with n.mu held.
func (n *Node) becomeFollower(term int, leaderID string) {
	if term > n.currentTerm {
		slog.Info("stepping down to follower", "id", n.config.NodeID,
			"old_term", n.currentTerm, "new_term", term)
		n.currentTerm = term
		n.votedFor = ""
		n.savePersistentState()
	}
	n.state = Follower
	n.leaderID = leaderID
	n.resetElectionTimer()
}

// becomeLeader transitions the node to leader after winning an election.
// Must be called with n.mu held.
func (n *Node) becomeLeader() {
	if n.state != Candidate {
		return // guard against duplicate becomeLeader calls
	}
	slog.Info("became leader", "id", n.config.NodeID, "term", n.currentTerm)
	n.state = Leader
	n.leaderID = n.config.NodeID

	// Initialise per-peer tracking indices
	n.nextIndex = make(map[string]int)
	n.matchIndex = make(map[string]int)
	lastIndex := n.raftLog.LastIndex()
	for _, peer := range n.config.Peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	n.resetHeartbeatTimer()
}

// ── Timers ────────────────────────────────────────────────────────────────────

// resetElectionTimer arms a randomised election timeout.
// Randomness is essential in Raft: without it all nodes would start
// elections simultaneously and nobody would ever win.
// Must be called with n.mu held.
func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	jitter := time.Duration(rand.Int63n(int64(ElectionTimeoutMax - ElectionTimeoutMin)))
	timeout := ElectionTimeoutMin + jitter
	n.electionTimer = time.AfterFunc(timeout, n.onElectionTimeout)
}

// resetHeartbeatTimer arms the periodic heartbeat for the leader.
// Must be called with n.mu held.
func (n *Node) resetHeartbeatTimer() {
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	n.heartbeatTimer = time.AfterFunc(HeartbeatInterval, n.onHeartbeatTimeout)
}

func (n *Node) onElectionTimeout() {
	select {
	case <-n.stopCh:
		return
	default:
		n.startElection()
	}
}

func (n *Node) onHeartbeatTimeout() {
	select {
	case <-n.stopCh:
		return
	default:
	}
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	n.resetHeartbeatTimer()
	n.mu.Unlock()
	n.replicateToAll()
}

// ── Committing and applying ───────────────────────────────────────────────────

// updateCommitIndex advances commitIndex when a majority of nodes have
// replicated an entry. Only the leader calls this.
// Must be called with n.mu held.
func (n *Node) updateCommitIndex() {
	lastIndex := n.raftLog.LastIndex()
	clusterSize := len(n.config.Peers) + 1

	for candidate := n.commitIndex + 1; candidate <= lastIndex; candidate++ {
		// Count nodes that have this entry (self counts as 1)
		count := 1
		for _, matchIdx := range n.matchIndex {
			if matchIdx >= candidate {
				count++
			}
		}
		entry, err := n.raftLog.GetEntry(candidate)
		if err != nil {
			break
		}
		// Raft safety rule: only commit entries from the current term
		if count > clusterSize/2 && entry.Term == n.currentTerm {
			n.commitIndex = candidate
			slog.Info("entry committed", "index", candidate, "command", entry.Command)
		} else {
			break
		}
	}
	n.applyCommitted()
}

// applyCommitted applies all committed-but-not-yet-applied entries to the KV store.
// Must be called with n.mu held.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry, err := n.raftLog.GetEntry(n.lastApplied)
		if err != nil {
			slog.Error("failed to read log entry", "index", n.lastApplied, "error", err)
			continue
		}
		if entry.Command == "" {
			continue // dummy entry at index 0
		}
		if err := n.store.Apply(entry.Command); err != nil {
			slog.Error("failed to apply command", "command", entry.Command, "error", err)
		} else {
			slog.Info("applied to store", "index", n.lastApplied, "command", entry.Command)
		}
	}
}

// ── Persistence ───────────────────────────────────────────────────────────────

func (n *Node) savePersistentState() {
	state := persistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor}
	data, _ := json.Marshal(state)
	path := filepath.Join(n.config.DataDir, "state.json")
	os.WriteFile(path, data, 0644)
}

func (n *Node) loadPersistentState() error {
	path := filepath.Join(n.config.DataDir, "state.json")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil // first start, use zero values
	}
	if err != nil {
		return err
	}
	var state persistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	return nil
}

// GetAll returns a snapshot of every key-value pair currently in the store.
func (n *Node) GetAll() map[string]string {
	return n.store.GetAll()
}

// GetNodeID returns the unique identifier of this node.
func (n *Node) GetNodeID() string {
	return n.config.NodeID
}
