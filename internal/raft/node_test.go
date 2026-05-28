package raft

import (
	"os"
	"testing"

	"github.com/Akiyoshi02/raft-kv-store/internal/kvstore"
)

// makeTestNode creates a node backed by a temp directory.
// The returned cleanup function removes the temp directory after the test.
func makeTestNode(t *testing.T, id, addr string, peers []string) (*Node, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "raft-node-*")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{NodeID: id, Address: addr, Peers: peers, DataDir: dir}
	node, err := NewNode(cfg, kvstore.New())
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	return node, func() {
		node.Stop()
		os.RemoveAll(dir)
	}
}

func TestNewNodeStartsAsFollower(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001",
		[]string{"localhost:8002", "localhost:8003"})
	defer cleanup()
	node.Start()

	state, term := node.GetState()
	if state != Follower {
		t.Fatalf("expected Follower, got %s", state)
	}
	if term != 0 {
		t.Fatalf("expected term 0, got %d", term)
	}
}

func TestVoteGrantedToFirstCandidate(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", []string{"localhost:8002"})
	defer cleanup()
	node.Start()

	reply := node.HandleRequestVote(RequestVoteArgs{
		Term: 1, CandidateID: "node2",
	})
	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted to the first candidate")
	}
	if reply.Term != 1 {
		t.Fatalf("expected term 1, got %d", reply.Term)
	}
}

func TestVoteRejectedForStaleTerm(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", []string{"localhost:8002"})
	defer cleanup()
	node.Start()

	// Advance our term to 5
	node.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node2"})

	// A request from term 3 should be rejected
	reply := node.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "node3"})
	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected for stale term")
	}
}

func TestNoDoubleVotingInSameTerm(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001",
		[]string{"localhost:8002", "localhost:8003"})
	defer cleanup()
	node.Start()

	// Vote for node2
	reply1 := node.HandleRequestVote(RequestVoteArgs{
		Term: 1, CandidateID: "node2",
	})
	if !reply1.VoteGranted {
		t.Fatal("expected first vote to be granted")
	}

	// node3 asks for a vote in the same term — must be rejected
	reply2 := node.HandleRequestVote(RequestVoteArgs{
		Term: 1, CandidateID: "node3",
	})
	if reply2.VoteGranted {
		t.Fatal("expected vote to be denied: already voted this term")
	}
}

func TestHeartbeatAccepted(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", []string{"localhost:8002"})
	defer cleanup()
	node.Start()

	reply := node.HandleAppendEntries(AppendEntriesArgs{
		Term: 1, LeaderID: "node2",
	})
	if !reply.Success {
		t.Fatal("expected heartbeat to be accepted")
	}
	if node.GetLeaderID() != "node2" {
		t.Fatalf("expected leader to be node2, got %q", node.GetLeaderID())
	}
}

func TestEntriesAppliedToStore(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", []string{"localhost:8002"})
	defer cleanup()
	node.Start()

	// Leader sends two entries and tells us both are committed
	node.HandleAppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node2",
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: "SET name Alice"},
			{Index: 2, Term: 1, Command: "SET city London"},
		},
		LeaderCommit: 2,
	})

	val, ok := node.GetValue("name")
	if !ok || val != "Alice" {
		t.Fatalf("expected name=Alice, got %q (found=%v)", val, ok)
	}
	val, ok = node.GetValue("city")
	if !ok || val != "London" {
		t.Fatalf("expected city=London, got %q (found=%v)", val, ok)
	}
}

func TestPersistentStateAfterRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-persist-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{
		NodeID:  "node1",
		Address: "localhost:8001",
		Peers:   []string{"localhost:8002"},
		DataDir: dir,
	}

	// First run: vote in term 3
	n1, _ := NewNode(cfg, kvstore.New())
	n1.Start()
	n1.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "node2"})
	n1.Stop()

	// Second run: must remember the term
	n2, err := NewNode(cfg, kvstore.New())
	if err != nil {
		t.Fatal(err)
	}
	n2.Start()
	defer n2.Stop()

	_, term := n2.GetState()
	if term != 3 {
		t.Fatalf("expected term 3 after restart, got %d", term)
	}
}
