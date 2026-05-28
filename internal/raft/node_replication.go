package raft

import (
	"bytes"
	"encoding/json"
	"log/slog"
)

// replicateToAll sends AppendEntries RPCs to every peer.
// Used for both heartbeats (empty entries) and real log replication.
func (n *Node) replicateToAll() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	peers := n.config.Peers
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(peer)
	}
}

// replicateToPeer sends an AppendEntries RPC to a single peer and handles the reply.
func (n *Node) replicateToPeer(peer string) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := 0

	if prevLogIndex > 0 {
		entry, err := n.raftLog.GetEntry(prevLogIndex)
		if err != nil {
			n.mu.Unlock()
			return
		}
		prevLogTerm = entry.Term
	}

	entries := n.raftLog.GetEntriesFrom(nextIdx)

	args := AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.config.NodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.sendAppendEntries(peer, args)
	if err != nil {
		return // peer unreachable, will retry on next heartbeat
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Step down if we see a higher term
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term, "")
		return
	}

	if n.state != Leader {
		return
	}

	if reply.Success {
		// Advance our record of how far this peer's log has caught up
		if len(args.Entries) > 0 {
			lastSent := args.Entries[len(args.Entries)-1].Index
			if lastSent+1 > n.nextIndex[peer] {
				n.nextIndex[peer] = lastSent + 1
			}
			if lastSent > n.matchIndex[peer] {
				n.matchIndex[peer] = lastSent
			}
			n.updateCommitIndex()
		}
	} else {
		// Log inconsistency — back up one step and retry
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
		go n.replicateToPeer(peer)
	}
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
// Called for both heartbeats and real log entries.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.currentTerm, Success: false}

	// Reject leaders from an older term
	if args.Term < n.currentTerm {
		return reply
	}

	// Valid message from a current or newer leader — accept it
	if args.Term > n.currentTerm || n.state != Follower {
		n.becomeFollower(args.Term, args.LeaderID)
	} else {
		n.leaderID = args.LeaderID
		n.resetElectionTimer()
	}

	// Consistency check: our log must match the leader's at prevLogIndex
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > n.raftLog.LastIndex() {
			return reply // we're missing entries the leader expects us to have
		}
		entry, err := n.raftLog.GetEntry(args.PrevLogIndex)
		if err != nil || entry.Term != args.PrevLogTerm {
			return reply // our log diverges from the leader's
		}
	}

	// Merge the leader's entries into our log
	for i, newEntry := range args.Entries {
		if newEntry.Index <= n.raftLog.LastIndex() {
			existing, err := n.raftLog.GetEntry(newEntry.Index)
			if err == nil && existing.Term != newEntry.Term {
				// Conflict: our entry and the leader's entry disagree at this index.
				// Discard ours and everything after it, then append from the leader.
				n.raftLog.TruncateFrom(newEntry.Index)
				if err := n.raftLog.Append(args.Entries[i:]...); err != nil {
					slog.Error("failed to append after truncate", "error", err)
					return reply
				}
				break
			}
		} else {
			// We don't have this entry yet — append from here onwards
			if err := n.raftLog.Append(args.Entries[i:]...); err != nil {
				slog.Error("failed to append entries", "error", err)
				return reply
			}
			break
		}
	}

	// Advance our commit index to match the leader's
	if args.LeaderCommit > n.commitIndex {
		lastNewIndex := n.raftLog.LastIndex()
		if args.LeaderCommit < lastNewIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
		n.applyCommitted()
	}

	reply.Success = true
	return reply
}

// sendAppendEntries sends an AppendEntries RPC to a single peer over HTTP.
func (n *Node) sendAppendEntries(peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	data, err := json.Marshal(args)
	if err != nil {
		return reply, err
	}
	resp, err := n.httpClient.Post(
		"http://"+peer+"/raft/append",
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
