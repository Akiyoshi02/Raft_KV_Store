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

	"github.com/Akiyoshi02/Raft_KV_Store/internal/kvstore"
)

// persistentState holds fields that must survive a crash.
// These are saved to disk before responding to any RPC.
type persistentState struct {
	CurrentTerm int    `json:"current_term"`
	VotedFor    string `json:"voted_for"`
	CommitIndex int    `json:"commit_index"`
}

// Node is a single member of the Raft cluster.
type Node struct {
	mu sync.Mutex

	config Config

	// Persistent state - must be saved to disk before responding to RPCs
	currentTerm int
	votedFor    string // NodeID we voted for this term, "" if none

	// State tracked on all nodes. commitIndex is persisted so the in-memory
	// store can be rebuilt safely after restart.
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
	electionGen    uint64

	// Lifecycle
	stopCh   chan struct{}
	stopOnce sync.Once
	stateCh  chan struct{}
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
		httpClient: &http.Client{Timeout: RaftRPCTimeout},
		stopCh:     make(chan struct{}),
		stateCh:    make(chan struct{}),
	}

	if err := n.loadPersistentState(); err != nil {
		raftLog.Close()
		return nil, fmt.Errorf("load persistent state: %w", err)
	}
	if err := n.applyCommitted(); err != nil {
		raftLog.Close()
		return nil, fmt.Errorf("replay committed entries: %w", err)
	}

	return n, nil
}

// Start begins the node's operation by arming the election timer.
func (n *Node) Start() {
	n.mu.Lock()
	if len(n.config.Peers) == 0 {
		n.resetElectionTimer()
	} else {
		n.resetStartupTimer()
	}
	n.mu.Unlock()
	slog.Info("node started", "id", n.config.NodeID, "address", n.config.Address)
}

// Stop shuts the node down cleanly.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		n.mu.Lock()
		if n.electionTimer != nil {
			n.electionTimer.Stop()
		}
		if n.heartbeatTimer != nil {
			n.heartbeatTimer.Stop()
		}
		n.notifyStateChange()
		n.mu.Unlock()
		if err := n.raftLog.Close(); err != nil {
			slog.Error("failed to close raft log", "id", n.config.NodeID, "error", err)
		}
		slog.Info("node stopped", "id", n.config.NodeID)
	})
}

// Submit accepts a write command from a client.
// Only the leader can accept writes — returns an error if this node is not the leader.
func (n *Node) Submit(command string) error {
	if err := kvstore.Validate(command); err != nil {
		return fmt.Errorf("invalid command: %w", err)
	}

	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return fmt.Errorf("not leader: current leader is %q", n.leaderID)
	}

	entry := LogEntry{
		Index:   n.raftLog.LastIndex() + 1,
		Term:    n.currentTerm,
		Command: command,
	}
	if err := n.raftLog.Append(entry); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("append to log: %w", err)
	}

	slog.Info("submitted entry", "index", entry.Index, "command", command)
	if err := n.updateCommitIndex(); err != nil {
		n.mu.Unlock()
		return err
	}
	if n.commitIndex >= entry.Index {
		n.mu.Unlock()
		return nil
	}

	stateCh := n.stateCh
	n.mu.Unlock()
	n.replicateToAll()

	timer := time.NewTimer(SubmitTimeout)
	defer timer.Stop()

	for {
		select {
		case <-stateCh:
			n.mu.Lock()
			if n.commitIndex >= entry.Index {
				n.mu.Unlock()
				return nil
			}
			if n.state != Leader {
				leaderID := n.leaderID
				n.mu.Unlock()
				return fmt.Errorf("leadership lost before entry %d committed: current leader is %q", entry.Index, leaderID)
			}
			stateCh = n.stateCh
			n.mu.Unlock()
		case <-timer.C:
			return fmt.Errorf("entry %d was not committed before timeout", entry.Index)
		case <-n.stopCh:
			return fmt.Errorf("node stopped before entry %d committed", entry.Index)
		}
	}
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
func (n *Node) becomeFollower(term int, leaderID string) error {
	var persistErr error
	if term > n.currentTerm {
		slog.Info("stepping down to follower", "id", n.config.NodeID,
			"old_term", n.currentTerm, "new_term", term)
		n.currentTerm = term
		n.votedFor = ""
		if err := n.savePersistentState(); err != nil {
			persistErr = err
		}
	}
	n.state = Follower
	n.leaderID = leaderID
	n.notifyStateChange()
	n.resetElectionTimer()
	return persistErr
}

// becomeLeader transitions the node to leader after winning an election.
// Must be called with n.mu held.
func (n *Node) becomeLeader() error {
	if n.state != Candidate {
		return nil // guard against duplicate becomeLeader calls
	}
	slog.Info("became leader", "id", n.config.NodeID, "term", n.currentTerm)
	n.state = Leader
	n.leaderID = n.config.NodeID
	n.notifyStateChange()

	// Commit a current-term no-op entry after election. This lets the leader
	// safely commit any inherited entries from older terms without waiting for
	// a client write.
	entry := LogEntry{Index: n.raftLog.LastIndex() + 1, Term: n.currentTerm}
	if err := n.raftLog.Append(entry); err != nil {
		n.state = Follower
		n.leaderID = ""
		n.notifyStateChange()
		n.resetElectionTimer()
		return fmt.Errorf("append leader no-op entry: %w", err)
	}

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
	if err := n.updateCommitIndex(); err != nil {
		n.state = Follower
		n.leaderID = ""
		n.notifyStateChange()
		n.resetElectionTimer()
		return err
	}
	n.resetHeartbeatTimer()
	return nil
}

// ── Timers ────────────────────────────────────────────────────────────────────

// resetElectionTimer arms a randomised election timeout.
// Randomness is essential in Raft: without it all nodes would start
// elections simultaneously and nobody would ever win.
// Must be called with n.mu held.
func (n *Node) resetElectionTimer() {
	n.resetElectionTimerBetween(ElectionTimeoutMin, ElectionTimeoutMax)
}

// resetStartupTimer gives a multi-node process time to become reachable after
// startup before it campaigns. Once cluster traffic arrives, the regular
// election timeout is used so leader failover remains fast.
func (n *Node) resetStartupTimer() {
	n.resetElectionTimerBetween(StartupTimeoutMin, StartupTimeoutMax)
}

func (n *Node) resetElectionTimerBetween(minimum, maximum time.Duration) {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	n.electionGen++
	generation := n.electionGen
	jitter := time.Duration(rand.Int63n(int64(maximum - minimum)))
	timeout := minimum + jitter
	n.electionTimer = time.AfterFunc(timeout, func() {
		n.onElectionTimeout(generation)
	})
}

// resetHeartbeatTimer arms the periodic heartbeat for the leader.
// Must be called with n.mu held.
func (n *Node) resetHeartbeatTimer() {
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	n.heartbeatTimer = time.AfterFunc(HeartbeatInterval, n.onHeartbeatTimeout)
}

func (n *Node) onElectionTimeout(generation uint64) {
	select {
	case <-n.stopCh:
		return
	default:
		n.startElection(generation)
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
func (n *Node) updateCommitIndex() error {
	lastIndex := n.raftLog.LastIndex()
	clusterSize := len(n.config.Peers) + 1

	for candidate := lastIndex; candidate > n.commitIndex; candidate-- {
		entry, err := n.raftLog.GetEntry(candidate)
		if err != nil {
			return err
		}
		// Raft safety rule: advance through an entry from the current term.
		// Earlier entries become committed implicitly when this succeeds.
		if entry.Term != n.currentTerm {
			continue
		}

		// Count nodes that have this entry (self counts as 1)
		count := 1
		for _, matchIdx := range n.matchIndex {
			if matchIdx >= candidate {
				count++
			}
		}
		if count > clusterSize/2 {
			oldCommitIndex := n.commitIndex
			n.commitIndex = candidate
			if err := n.savePersistentState(); err != nil {
				n.commitIndex = oldCommitIndex
				return fmt.Errorf("persist commit index: %w", err)
			}
			n.notifyStateChange()
			slog.Info("entry committed", "index", candidate, "command", entry.Command)
			break
		}
	}
	return n.applyCommitted()
}

// applyCommitted applies all committed-but-not-yet-applied entries to the KV store.
// Must be called with n.mu held.
func (n *Node) applyCommitted() error {
	for n.lastApplied < n.commitIndex {
		nextIndex := n.lastApplied + 1
		entry, err := n.raftLog.GetEntry(nextIndex)
		if err != nil {
			return fmt.Errorf("read log entry %d: %w", nextIndex, err)
		}
		if entry.Command == "" {
			n.lastApplied = nextIndex
			continue // dummy entry at index 0
		}
		if err := n.store.Apply(entry.Command); err != nil {
			return fmt.Errorf("apply command at index %d: %w", nextIndex, err)
		}
		n.lastApplied = nextIndex
		slog.Info("applied to store", "index", n.lastApplied, "command", entry.Command)
	}
	return nil
}

// ── Persistence ───────────────────────────────────────────────────────────────

func (n *Node) notifyStateChange() {
	close(n.stateCh)
	n.stateCh = make(chan struct{})
}

func (n *Node) savePersistentState() error {
	state := persistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor}
	state.CommitIndex = n.commitIndex
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal persistent state: %w", err)
	}
	path := filepath.Join(n.config.DataDir, "state.json")
	file, err := os.CreateTemp(n.config.DataDir, "state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary persistent state: %w", err)
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	if _, err := file.Write(data); err != nil {
		file.Close()
		return fmt.Errorf("write persistent state: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync persistent state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close persistent state: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace persistent state: %w", err)
	}
	return nil
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
	if state.CommitIndex < 0 || state.CommitIndex > n.raftLog.LastIndex() {
		return fmt.Errorf("commit index %d exceeds log length %d", state.CommitIndex, n.raftLog.LastIndex())
	}
	n.commitIndex = state.CommitIndex
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
