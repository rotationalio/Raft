package raft

import (
	"Raft/api"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	Id      string `json:"id"`
	Port    int    `json:"port"`
	Address string
	Client  api.RaftClient
}

const (
	ElectionTimeoutTick time.Duration = time.Second * 8
	HeartbeatTick       time.Duration = time.Second * 5
)

type Ticker struct {
	period  time.Duration
	timeout *time.Ticker
	ch      <-chan time.Time
}

func NewTicker(period time.Duration) *Ticker {
	timeout := *time.NewTicker(period)
	return &Ticker{period: period, timeout: &timeout, ch: timeout.C}
}

type RaftServer struct {
	api.UnimplementedRaftServer

	// channels
	timeout  *Ticker
	shutdown chan os.Signal
	errC     chan error

	// Self identification
	id      string // The uuid uniquely identifying the raft node.
	port    int    // The port the node is serving on.
	address string
	srv     *grpc.Server
	quorum  []Peer

	// Persistent state on all servers (Updated on stable storage before responding to RPCs).
	currentTerm int32        // latest term server has seen (initialized to 0 on first boot, increases monotonically).
	votedFor    string       // candidate Id that received vote in current term (or nil if none).
	log         []*api.Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1).

	// Volatile state on all servers
	commitIndex int64 // index of the highest log entry known to be committed (initialized to 0, increases monotonically).
	lastApplied int64 // index of highest log entry applied to state machine (initialized to 0, increases monotonically).
	state       State // The current state of the node, either leader, candidate or follower.

	// Volatile state on leaders (Reinitialized after election).
	// TODO: change these to slices when dealing with multiple replica state
	nextIndex  int64 // for each server, index of the next log entry to send to that server (initialized to leader's last log index + 1).
	matchIndex int64 // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically).
}

func ServeRaft(port int, id string) (err error) {
	// Create a new gRPC Raft server.
	raftNode := RaftServer{srv: grpc.NewServer()}
	raftNode.id = id
	raftNode.port = port
	if err = raftNode.initialize(false); err != nil {
		return err
	}
	api.RegisterRaftServer(raftNode.srv, &raftNode)

	// Create a channel to receive interrupt signals and shutdown the server.
	shutdown := make(chan os.Signal, 2)
	signal.Notify(shutdown, os.Interrupt)
	raftNode.shutdown = shutdown
	raftNode.address = fmt.Sprintf("localhost:%d", port)

	// Create a socket on the specified port.
	var listener net.Listener
	if listener, err = net.Listen("tcp", raftNode.address); err != nil {
		log.Error().Msg(err.Error())
		return err
	}

	// Start the oneBigPipe event loop and serve in a go function with the created socket.
	log.Info().Msg(fmt.Sprintf("serving %v on %v", id, raftNode.address))
	go raftNode.oneBigPipe()
	err = raftNode.Serve(listener)
	return err
}

func (s *RaftServer) Serve(listener net.Listener) error {
	go func() {
		err := <-s.errC
		log.Error().Msg(fmt.Sprintf("encountered error: %v", err))
		s.gracefulShutdown()
	}()
	err := s.srv.Serve(listener)
	return err
}

//
func (s *RaftServer) oneBigPipe() {
	go func() {
		for {
			select {
			case <-s.shutdown:
				s.gracefulShutdown()
				return
			case <-s.timeout.ch:
				if s.state == leader {
					log.Info().Msg("HEARTBEAT")
					go s.sendHeartbeat()
				} else {
					log.Info().Msg("ELECTION")
					go s.startElection()
				}
			}
		}
	}()
}

// Stope the server, log and exit.
func (s *RaftServer) gracefulShutdown() {
	fmt.Printf("\nStopping server on on %v\n", s.address)
	s.srv.Stop()
	os.Exit(1)
}

func (s *RaftServer) sendHeartbeat() {
	var stream api.Raft_AppendEntriesClient
	in := &api.AppendEntriesRequest{Id: s.id, Term: s.currentTerm}
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
	votesReceived := 1
	s.votedFor = s.id
	defer s.resetVotedFor()
	votesNeeded := len(s.quorum)
	log.Info().Msg(fmt.Sprintf("starting election, incremented term to %v", s.currentTerm))

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
			log.Info().Msg("reverted to follower, abandoning election")
			return
		}
		if out, err = peer.Client.RequestVote(context.TODO(), in); err != nil {
			s.errC <- err
			return
		}

		if out.VoteGranted {
			votesReceived++
			log.Info().Msg(fmt.Sprintf("received successful vote from %v, %v/%v", peer.Id, votesReceived, votesNeeded))
			if votesReceived >= votesNeeded {
				log.Info().Msg(fmt.Sprintf("received %v successful votes, election successful", votesNeeded))
				// TODO: is this the correct way to handle this?
				if s.state == follower {
					log.Info().Msg("reverted to follower, abandoning election")
					return
				} else {
					if err = s.becomeLeader(); err != nil {
						s.errC <- err
						return
					}

					log.Info().Msg("sending first heartbeat as leader")
					s.sendHeartbeat()

					if err = s.initialize(true); err != nil {
						s.errC <- err
						return
					}
					return
				}
			}
		} else {
			log.Info().Msg(fmt.Sprintf("peer %v rejected vote, current votes: %v", peer.Id, votesReceived))
		}
	}
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
				log.Info().Msg("heartbeat received")
				if s.state != follower {
					// If the leader's term (included in it's RPC) is at least as large
					// as the candidate's current term, then the candidate recognizes the
					// leader as legitimate and returns to follower state.
					if req.Term >= s.currentTerm {
						log.Info().Msg("heartbeat received from new leader, reverting to follower")
						s.becomeFollower()
						s.timeout = NewTicker(s.electionTick())
						return stream.SendAndClose(&api.AppendEntriesReply{Success: true})
					} else {
						// If the term in the RPC is smaller than the candidate's term than the
						// candidate rejects the RPC and continues in candidate state
						log.Info().Msg(fmt.Sprintf("request term %v smaller than current term %v", req.Term, s.currentTerm))
						return stream.SendAndClose(&api.AppendEntriesReply{Success: false})
					}
				}
				// If current state is follower, log that you have received a heartbeat from the leader,
				// reset the election timeout and return success.
				log.Info().Msg("heartbeat received, resetting timeout")
				s.timeout = NewTicker(s.electionTick())
				return stream.SendAndClose(&api.AppendEntriesReply{Success: true})
			}
		}
		if err == io.EOF {
			log.Info().Msg(fmt.Sprintf("current log: %v", s.log))
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
	if in.Term >= s.currentTerm {
		if s.votedFor == "" || s.votedFor == in.CandidateId {
			if in.LastLogIndex >= s.lastApplied {
				out.VoteGranted = true
				return out, nil
			}

		}
	}
	out.VoteGranted = false
	return out, nil
}

func (s *RaftServer) initialize(electedLeader bool) (err error) {
	if !electedLeader {
		s.currentTerm = 0
		s.votedFor = ""
		s.commitIndex = 0
		s.lastApplied = 0
		s.errC = make(chan error)
		s.becomeFollower()
		s.timeout = NewTicker(s.electionTick())

		if err = s.findQuorum(); err != nil {
			return err
		}
	} else {
		s.nextIndex = int64(len(s.log) + 1)
		s.matchIndex = 0
		s.timeout = NewTicker(HeartbeatTick)
	}
	return nil
}

func (s *RaftServer) electionTick() time.Duration {
	rand.Seed(int64(s.port))
	electionTimeoutEpsilon := time.Millisecond * time.Duration(rand.Intn(150))
	timeoutDuration := ElectionTimeoutTick + electionTimeoutEpsilon
	return timeoutDuration
}

func (s *RaftServer) findQuorum() (err error) {
	jsonBytes, err := ioutil.ReadFile("config.json")
	if err != nil {
		return err
	}

	var peers []Peer
	if err = json.Unmarshal(jsonBytes, &peers); err != nil {
		return err
	}

	for _, peer := range peers {
		if peer.Id != s.id {
			peer.Address = fmt.Sprintf("localhost:%d", peer.Port)
			if peer.Client, err = CreateClient(peer.Address); err != nil {
				return err
			}
			s.quorum = append(s.quorum, peer)
		}
	}

	log.Info().Msg(fmt.Sprintf("Quorum:  %v", s.quorum))
	return nil
}

func CreateClient(address string) (client api.RaftClient, err error) {
	log.Info().Msg("Creating Client")

	opts := grpc.WithTransportCredentials(insecure.NewCredentials())

	var conn *grpc.ClientConn
	if conn, err = grpc.Dial(address, opts); err != nil {
		return nil, err
	}

	client = api.NewRaftClient(conn)
	return client, nil
}
