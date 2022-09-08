package raft

import (
	"Raft/api"
	"context"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"
)

func (s *RaftServer) sendHeartbeat() {
	fmt.Println("sending heartbeat")
	var stream api.Raft_AppendEntriesClient
	in := &api.AppendEntriesRequest{Id: s.id, Term: s.currentTerm, Entries: nil}
	var out *api.AppendEntriesReply

	var err error
	for _, peer := range s.quorum {
		if stream, err = peer.Client.AppendEntries(context.TODO()); err != nil {
			s.errC <- err
			return
		}

		stream.Send(in)
		if out, err = stream.CloseAndRecv(); err != nil {
			if err != io.EOF {
				s.errC <- err
			} else {
				log.Error().Msg("EOF")
				continue
			}
		}

		if !out.Success {
			log.Error().Msg("Heartbeat unsuccessful")
		}
	}
}

func (s *RaftServer) startElection() {
	s.becomeCandidate()
	s.currentTerm++

	// vote for self
	s.votedFor = s.id
	fmt.Printf("starting election, incremented term to %v\n", s.currentTerm)
}

func (s *RaftServer) conductElection() (success bool) {
	votesReceived := 1
	votesNeeded := len(s.quorum) // A majority

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

	var err error
	for _, peer := range s.quorum {
		if s.state == follower {
			log.Debug().Msg("reverted to follower, abandoning election")
			return false
		}
		if out, err = peer.Client.RequestVote(context.TODO(), in); err != nil {
			s.errC <- err
			return false
		}

		if out.VoteGranted {
			votesReceived++
			fmt.Printf("received successful vote from %v, %v/%v\n", peer.Id, votesReceived, votesNeeded)
			if votesReceived >= votesNeeded {
				// TODO: is this the correct way to handle this?
				if s.state == follower {
					fmt.Printf("reverted to follower, abandoning election\n")
					return false
				} else {
					fmt.Printf("received %v successful votes, election successful\n", votesNeeded)
					return true
				}
			}
		} else {
			fmt.Printf("peer %v rejected vote, current votes: %v\n", peer.Id, votesReceived)
		}
	}
	return false
}

func (s *RaftServer) resetVotedFor() {
	s.votedFor = ""
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
	var req *api.AppendEntriesRequest
	for {
		req, err = stream.Recv()
		if req.Entries == nil {
			if err == nil {
				// While waiting for votes a candidate may receive an AppendEntries RPC
				// from another server claiming to be leader.
				fmt.Printf("heartbeat received: %v\n", req)
				if s.state != follower {
					// If the leader's term (included in it's RPC) is at least as large
					// as the candidate's current term, then the candidate recognizes the
					// leader as legitimate and returns to follower state.
					if req.Term >= s.currentTerm {
						fmt.Printf("received successful heartbeat from %s\n", req.Id)
						s.becomeFollower()
						s.timeout.Reset(s.electionTick())
						return stream.SendAndClose(&api.AppendEntriesReply{Success: true})
					} else {
						// If the term in the RPC is smaller than the candidate's term than the
						// candidate rejects the RPC and continues in candidate state
						fmt.Printf("request term %v smaller than current term %v", req.Term, s.currentTerm)
						return stream.SendAndClose(&api.AppendEntriesReply{Success: false})
					}
				}
				// If current state is follower, log that you have received a heartbeat from the leader,
				// reset the election timeout and return success.
				log.Debug().Msg("heartbeat received, resetting timeout")
				s.timeout.Reset(s.electionTick())
				return stream.SendAndClose(&api.AppendEntriesReply{Success: true})
			} else {
				fmt.Println(err)
			}
		}
		if err == io.EOF {
			fmt.Printf("current log: %v\n", s.log)
			return stream.SendAndClose(&api.AppendEntriesReply{Success: true})
		}
		if err != nil {
			s.errC <- err
		}
		s.log = append(s.log, req.Entries...)
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
	out = &api.VoteReply{Id: s.id, Term: s.currentTerm}
	s.Lock()
	defer s.Unlock()
	if in.Term >= s.currentTerm {
		if s.votedFor == "" || s.votedFor == in.CandidateId {
			if in.LastLogIndex >= s.lastApplied {
				out.VoteGranted = true
				s.votedFor = in.CandidateId
				return out, nil
			}
		}
	}
	out.VoteGranted = false
	return out, nil
}
