package raft

import (
	"Raft/api"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog/log"
)

func (s *RaftServer) sendHeartbeat() {
	s.Lock()
	startingTerm := s.currentTerm
	s.Unlock()

	var err error
	var out *api.AppendEntriesReply
	for _, peer := range s.quorum {
		var stream api.Raft_AppendEntriesClient
		if stream, err = peer.Client.AppendEntries(context.TODO()); err != nil {
			s.errC <- err
			return
		}

		in := &api.AppendEntriesRequest{Id: s.id, Term: s.currentTerm, LeaderId: s.id, Entries: nil}
		// TODO: separate go routine into it's own function
		go func(peer Peer) {
			stream.Send(in)
			if out, err = stream.CloseAndRecv(); err != nil {
				if err != io.EOF {
					s.errC <- err
					return
				} else {
					log.Error().Msg("EOF")
					return
				}
			}

			if out.Term > startingTerm {
				s.becomeFollower(out.Term)
				return
			}

			if !out.Success {
				log.Error().Msg("Heartbeat unsuccessful")
				return
			}
		}(peer)
	}
}

func (s *RaftServer) startElection() {
	var err error
	if err = s.becomeCandidate(); err != nil {
		s.errC <- err
		return
	}

	s.currentTerm++
	startingTerm := s.currentTerm
	s.lastHeartbeat = time.Now()

	// vote for self
	s.votedFor = s.id
	fmt.Printf("starting election, incremented term to %v\n", s.currentTerm)

	votesReceived := 1
	votesNeeded := (len(s.quorum) + 1) - len(s.quorum)/2

	var lastLogTerm int32
	if s.log == nil {
		lastLogTerm = 0
	} else {
		lastLogTerm = s.log[s.lastApplied].Term
	}

	var out *api.VoteReply
	in := &api.VoteRequest{
		Term:         s.currentTerm,
		CandidateId:  s.id,
		LastLogIndex: s.lastApplied,
		LastLogTerm:  lastLogTerm,
	}

	// TODO: separate go routine into it's own function
	for _, peer := range s.quorum {
		go func(peer Peer) {
			fmt.Printf("requesting vote from %s\n", peer.Id)
			if out, err = peer.Client.RequestVote(context.TODO(), in); err != nil {
				s.errC <- err
				return
			}
			fmt.Printf("%s\n", out)
			s.Lock()
			defer s.Unlock()

			if s.state != candidate {
				fmt.Println("reverted to follower, abandoning election")
				return
			}

			if out.Term > startingTerm {
				s.becomeFollower(out.Term)
				return
			} else if out.Term == startingTerm {
				if out.VoteGranted {
					votesReceived += 1
					fmt.Printf("Received new vote: %v/%v\n", votesReceived, votesNeeded)
					if votesReceived >= votesNeeded {
						s.becomeLeader()
						return
					}
				}
			}
		}(peer)
	}
	go s.runElectionTimer()
}

// Invoked by leader to replicate log entries (section 5.3 of the Raft whitepaper);
// also used as heartbeat (section 5.2 of the Raft whitepaper).
//
// Receiver implementation:
// 1. Reply false if term < currentTerm (section §5.1 of the Raft whitepaper).
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
//    whose term matches prevLogTerm (section §5.3 of the Raft whitepaper).
// 3. If an existing entry conflicts with a new one (same index
//    but different terms), delete the existing entry and all that
//    follow it (section §5.3 of the Raft whitepaper).
// 4. Append any new entries not already in the log.
// 5. If leaderCommit > commitIndex, set commitIndex =
//    min(leaderCommit, index of last new entry).
func (s *RaftServer) AppendEntries(stream api.Raft_AppendEntriesServer) (err error) {
	s.Lock()
	defer s.Unlock()

	if s.state == dead {
		return fmt.Errorf("node is dead")
	}

	var req *api.AppendEntriesRequest
	for {
		if req, err = stream.Recv(); err != nil {
			return err
		}

		if req.Term > s.currentTerm {
			s.becomeFollower(req.Term)
		}

		rep := &api.AppendEntriesReply{Success: false}
		if req.Term == s.currentTerm {
			if s.state != follower {
				s.becomeFollower(req.Term)
			}
			s.lastHeartbeat = time.Now()
			rep.Success = true
		}

		rep.Term = s.currentTerm
		return stream.SendAndClose(rep)
	}
}

// TODO: use the following to handle multi-replica case (tentative)
//for _, entry := range req.Entries {
// if i >= len(s.Log) {
//	s.Log = append(s.Log, entry)
// } else if entry.Term < s.CurrentTerm {
// 	return stream.SendAndClose(
// 		&api.AppendEntriesReply{
// 			Success: false,
// 		})
//} else if entry.Term != s.Log[i].Term {
// if entry.Term != s.Log[i].Term {
// 	s.Log = s.Log[:i]
// }
//}

// Invoked by candidates to gather votes (§5.2).
//
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1).
// 2. If votedFor is null or candidateId, and candidate’s log is at
//    least as up-to-date as receiver’s log, grant vote (sections §5.2, §5.4 of the
//    Raft whitepaper).
// TODO: implement for multi-replica version with leader election
func (s *RaftServer) RequestVote(ctx context.Context, in *api.VoteRequest) (out *api.VoteReply, err error) {
	s.Lock()
	defer s.Unlock()
	out = &api.VoteReply{Id: s.id, Term: s.currentTerm}

	if s.state == dead {
		return nil, fmt.Errorf("node is dead")
	}

	if in.Term > s.currentTerm {
		s.becomeFollower(in.Term)
	}

	if s.currentTerm == in.Term && (s.votedFor == in.CandidateId || s.votedFor == "") {
		out.VoteGranted = true
		s.votedFor = in.CandidateId
		s.lastHeartbeat = time.Now()
	} else {
		out.VoteGranted = false
	}
	out.Term = s.currentTerm
	return out, nil
}
