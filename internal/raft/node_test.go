package raft

import (
	"os"
	"testing"
	"time"

	"github.com/Akiyoshi02/Raft_KV_Store/internal/kvstore"
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

func TestSingleNodeElectsLeaderAndCommits(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", nil)
	defer cleanup()
	node.Start()

	deadline := time.Now().Add(ElectionTimeoutMax + time.Second)
	for !node.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !node.IsLeader() {
		t.Fatal("expected single node cluster to elect itself as leader")
	}

	if err := node.Submit("SET name Alice"); err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	value, ok := node.GetValue("name")
	if !ok || value != "Alice" {
		t.Fatalf("expected committed value name=Alice, got %q (found=%v)", value, ok)
	}
}

func TestMultiNodeStartupUsesElectionGracePeriod(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", []string{"localhost:8002"})
	defer cleanup()
	node.Start()

	time.Sleep(ElectionTimeoutMax + 100*time.Millisecond)

	state, term := node.GetState()
	if state != Follower || term != 0 {
		t.Fatalf("expected startup grace period to keep node as term-0 follower, got state=%s term=%d", state, term)
	}
}

func TestSubmitWaitsForCommit(t *testing.T) {
	const peer = "127.0.0.1:1"
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", []string{peer})
	defer cleanup()

	node.mu.Lock()
	node.currentTerm = 1
	node.state = Leader
	node.leaderID = node.config.NodeID
	node.nextIndex = map[string]int{peer: 1}
	node.matchIndex = map[string]int{peer: 0}
	node.mu.Unlock()

	result := make(chan error, 1)
	go func() {
		result <- node.Submit("SET pending value")
	}()

	select {
	case err := <-result:
		t.Fatalf("submit returned before quorum commit: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, ok := node.GetValue("pending"); ok {
		t.Fatal("uncommitted value was applied to the store")
	}

	node.mu.Lock()
	err := node.becomeFollower(2, "node2")
	node.mu.Unlock()
	if err != nil {
		t.Fatalf("step down failed: %v", err)
	}

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected submit to fail after leadership was lost")
		}
	case <-time.After(time.Second):
		t.Fatal("submit did not return after leadership was lost")
	}
}

func TestCommittedEntriesRestoredAfterRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-replay-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{NodeID: "node1", Address: "localhost:8001", DataDir: dir}
	n1, err := NewNode(cfg, kvstore.New())
	if err != nil {
		t.Fatal(err)
	}
	reply := n1.HandleAppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node2",
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: "SET name Alice"},
		},
		LeaderCommit: 1,
	})
	if !reply.Success {
		t.Fatal("expected append entries to succeed")
	}
	n1.Stop()

	n2, err := NewNode(cfg, kvstore.New())
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Stop()

	value, ok := n2.GetValue("name")
	if !ok || value != "Alice" {
		t.Fatalf("expected restored value name=Alice, got %q (found=%v)", value, ok)
	}
}

func TestLeaderCommitsCurrentTermEntryPastOlderTermEntry(t *testing.T) {
	const peer = "localhost:8002"
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", []string{peer})
	defer cleanup()

	node.mu.Lock()
	node.currentTerm = 2
	node.state = Leader
	node.leaderID = node.config.NodeID
	node.matchIndex = map[string]int{peer: 2}
	if err := node.raftLog.Append(
		LogEntry{Index: 1, Term: 1, Command: "SET old committed"},
		LogEntry{Index: 2, Term: 2, Command: "SET current committed"},
	); err != nil {
		node.mu.Unlock()
		t.Fatal(err)
	}
	if err := node.updateCommitIndex(); err != nil {
		node.mu.Unlock()
		t.Fatal(err)
	}
	commitIndex := node.commitIndex
	node.mu.Unlock()

	if commitIndex != 2 {
		t.Fatalf("expected commit index 2, got %d", commitIndex)
	}
	if value, ok := node.GetValue("old"); !ok || value != "committed" {
		t.Fatalf("expected older entry to commit implicitly, got %q (found=%v)", value, ok)
	}
	if value, ok := node.GetValue("current"); !ok || value != "committed" {
		t.Fatalf("expected current-term entry to commit, got %q (found=%v)", value, ok)
	}
}

func TestAppendEntriesReplyUsesUpdatedTerm(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", nil)
	defer cleanup()

	reply := node.HandleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: "node2"})
	if reply.Term != 3 {
		t.Fatalf("expected reply term 3, got %d", reply.Term)
	}
}

func TestLeaderNoOpCommitsInheritedEntryOnSingleNode(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", nil)
	defer cleanup()

	node.mu.Lock()
	node.currentTerm = 2
	node.state = Candidate
	if err := node.raftLog.Append(LogEntry{Index: 1, Term: 1, Command: "SET inherited value"}); err != nil {
		node.mu.Unlock()
		t.Fatal(err)
	}
	if err := node.becomeLeader(); err != nil {
		node.mu.Unlock()
		t.Fatal(err)
	}
	commitIndex := node.commitIndex
	node.mu.Unlock()

	if commitIndex != 2 {
		t.Fatalf("expected inherited entry and no-op to commit at index 2, got %d", commitIndex)
	}
	if value, ok := node.GetValue("inherited"); !ok || value != "value" {
		t.Fatalf("expected inherited entry to be applied, got %q (found=%v)", value, ok)
	}
}

func TestAppendEntriesRejectsNonSequentialIndex(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", nil)
	defer cleanup()

	reply := node.HandleAppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node2",
		Entries: []LogEntry{
			{Index: 2, Term: 1, Command: "SET skipped index"},
		},
	})
	if reply.Success {
		t.Fatal("expected malformed append entries request to fail")
	}
	if node.raftLog.LastIndex() != 0 {
		t.Fatalf("expected log to remain empty, got last index %d", node.raftLog.LastIndex())
	}
}

func TestSubmitRejectsInvalidCommand(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", nil)
	defer cleanup()

	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.mu.Unlock()

	if err := node.Submit("DELETE "); err == nil {
		t.Fatal("expected invalid command to be rejected")
	}
	if node.raftLog.LastIndex() != 0 {
		t.Fatalf("expected invalid command not to reach log, got last index %d", node.raftLog.LastIndex())
	}
}

func TestAppendEntriesRejectsInvalidCommand(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", nil)
	defer cleanup()

	reply := node.HandleAppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node2",
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: "DELETE "},
		},
	})
	if reply.Success {
		t.Fatal("expected invalid replicated command to be rejected")
	}
	if node.raftLog.LastIndex() != 0 {
		t.Fatalf("expected invalid command not to reach log, got last index %d", node.raftLog.LastIndex())
	}
}

func TestAppendEntriesRejectsConflictWithCommittedEntry(t *testing.T) {
	node, cleanup := makeTestNode(t, "node1", "localhost:8001", nil)
	defer cleanup()

	reply := node.HandleAppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node2",
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: "SET stable value"},
		},
		LeaderCommit: 1,
	})
	if !reply.Success {
		t.Fatal("expected initial append entries request to succeed")
	}

	reply = node.HandleAppendEntries(AppendEntriesArgs{
		Term:     2,
		LeaderID: "node3",
		Entries: []LogEntry{
			{Index: 1, Term: 2, Command: "SET stable overwritten"},
		},
	})
	if reply.Success {
		t.Fatal("expected committed log conflict to be rejected")
	}
	entry, err := node.raftLog.GetEntry(1)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Command != "SET stable value" {
		t.Fatalf("expected committed entry to remain unchanged, got %q", entry.Command)
	}
}
