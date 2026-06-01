package raft

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"sync"
)

// startElection begins a new leader election.
// Called when the election timer fires — meaning we haven't heard from
// a leader and we're going to try to become one ourselves.
func (n *Node) startElection(generation uint64) {
	n.mu.Lock()

	// Ignore an expired timer callback if a heartbeat reset the timer while
	// the callback was waiting to acquire the lock.
	if generation != n.electionGen || n.state == Leader {
		n.mu.Unlock()
		return
	}

	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.config.NodeID // vote for ourselves
	n.leaderID = ""
	if err := n.savePersistentState(); err != nil {
		slog.Error("failed to persist election state", "id", n.config.NodeID, "error", err)
		n.state = Follower
		n.resetElectionTimer()
		n.mu.Unlock()
		return
	}
	n.resetElectionTimer() // reset in case this election also times out

	term := n.currentTerm
	lastLogIndex := n.raftLog.LastIndex()
	lastLogTerm := n.raftLog.LastTerm()
	peers := n.config.Peers

	slog.Info("starting election", "id", n.config.NodeID, "term", term)

	n.mu.Unlock()

	votes := 1 // we vote for ourselves
	majority := (len(peers)+1)/2 + 1
	var votesMu sync.Mutex

	if votes >= majority {
		n.mu.Lock()
		if n.state == Candidate && n.currentTerm == term {
			if err := n.becomeLeader(); err != nil {
				slog.Error("failed to become leader", "id", n.config.NodeID, "error", err)
			}
		}
		n.mu.Unlock()
		n.replicateToAll()
		return
	}

	for _, peer := range peers {
		go func(peer string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.config.NodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			reply, err := n.sendRequestVote(peer, args)
			if err != nil {
				return // peer unreachable, not a problem
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// If we see a higher term, step down immediately
			if reply.Term > n.currentTerm {
				if err := n.becomeFollower(reply.Term, ""); err != nil {
					slog.Error("failed to persist higher term", "term", reply.Term, "error", err)
				}
				return
			}

			// Ignore stale replies from previous terms or if we've already won/lost
			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if reply.VoteGranted {
				votesMu.Lock()
				votes++
				currentVotes := votes
				votesMu.Unlock()

				if currentVotes >= majority && n.state == Candidate {
					if err := n.becomeLeader(); err != nil {
						slog.Error("failed to become leader", "id", n.config.NodeID, "error", err)
					}
					go n.replicateToAll() // send immediate heartbeats to assert leadership
				}
			}
		}(peer)
	}
}

// HandleRequestVote processes an incoming vote request from a candidate.
// This is called by the HTTP handler when another node wants our vote.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.currentTerm, VoteGranted: false}

	// Rule 1: reject candidates from an older term
	if args.Term < n.currentTerm {
		return reply
	}

	// Rule 2: if we see a higher term, update and step down
	if args.Term > n.currentTerm {
		if err := n.becomeFollower(args.Term, ""); err != nil {
			slog.Error("failed to persist vote request term", "term", args.Term, "error", err)
			reply.Term = n.currentTerm
			return reply
		}
	}

	// Rule 3: only vote if we haven't voted yet this term (or already
	// voted for this same candidate), AND the candidate's log is at
	// least as up-to-date as ours
	canVote := n.votedFor == "" || n.votedFor == args.CandidateID
	logOk := args.LastLogTerm > n.raftLog.LastTerm() ||
		(args.LastLogTerm == n.raftLog.LastTerm() &&
			args.LastLogIndex >= n.raftLog.LastIndex())

	if canVote && logOk {
		previousVote := n.votedFor
		n.votedFor = args.CandidateID
		if err := n.savePersistentState(); err != nil {
			n.votedFor = previousVote
			slog.Error("failed to persist vote", "to", args.CandidateID, "term", args.Term, "error", err)
			reply.Term = n.currentTerm
			return reply
		}
		n.resetElectionTimer() // hearing from a valid candidate resets our timer
		reply.VoteGranted = true
		slog.Info("granted vote", "to", args.CandidateID, "term", args.Term)
	}

	reply.Term = n.currentTerm
	return reply
}

// sendRequestVote sends a RequestVote RPC to a single peer over HTTP.
func (n *Node) sendRequestVote(peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	data, err := json.Marshal(args)
	if err != nil {
		return reply, err
	}
	resp, err := n.httpClient.Post(
		"http://"+peer+"/raft/vote",
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&reply)
	return reply, err
}
