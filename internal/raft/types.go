package raft

import "time"

// NodeState represents the current state of a Raft node.
// Every node is always in one of these three states.
type NodeState int

const (
	Follower  NodeState = iota // Passive - receives updates from leader
	Candidate                  // Running for election to become leader
	Leader                     // In charge - handles all writes
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a single record in the Raft log.
// Every write operation (SET, DELETE) becomes a LogEntry.
type LogEntry struct {
	Index   int    `json:"index"`   // Position in the log (1, 2, 3...)
	Term    int    `json:"term"`    // Which leader's term this was written in
	Command string `json:"command"` // The actual operation e.g. "SET name Alice"
}

// RequestVoteArgs is the message a Candidate sends to ask for a vote.
type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

// RequestVoteReply is the response to a vote request.
type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesArgs is the message the Leader sends to replicate log entries.
// When Entries is empty it acts as a heartbeat - just proving the leader is alive.
type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

// AppendEntriesReply is the response to an AppendEntries message.
type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

// Config holds everything a node needs to know about itself and the cluster.
type Config struct {
	NodeID  string   // Unique name for this node e.g. "node1"
	Address string   // This node's HTTP address e.g. "localhost:8001"
	Peers   []string // Addresses of the other nodes in the cluster
	DataDir string   // Folder for saving log to disk
}

// Timing constants that control election and heartbeat behaviour.
// These values follow the recommendations in the Raft paper.
const (
	HeartbeatInterval  = 50 * time.Millisecond
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
)