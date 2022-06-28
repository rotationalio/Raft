package raft

import (
	"Raft/api"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type State uint8

const (
	leader State = iota
	follower
	candidate
)

type Peer struct {
	Id      string `json:"id"`
	Port    int    `json:"port"`
	Address string
	Client  api.RaftClient
}

const (
	ElectionTimeoutTick time.Duration = time.Second * 5
	HeartbeatTick       time.Duration = time.Second * 2
)

type Ticker struct {
	period  time.Duration
	timeout time.Ticker
}

func NewTicker(period time.Duration) *Ticker {
	return &Ticker{period, *time.NewTicker(period)}
}

func (t *Ticker) Reset() {
	t.timeout = *time.NewTicker(t.period)
}

type RaftServer struct {
	api.UnimplementedRaftServer

	// channels
	shutdown  chan os.Signal
	heartbeat *Ticker

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

	// serve in a go function with the created socket.
	log.Info().Msg(fmt.Sprintf("serving %v on %v", id, raftNode.address))
	go raftNode.oneBigPipe()
	raftNode.srv.Serve(listener)
	return nil

	// TODO: Handle timeouts, votes, etc...
}

func (s *RaftServer) oneBigPipe() (err error) {
	err = nil
	go func() {
		for {
			select {
			case <-s.shutdown:
				s.gracefulShutdown()
			case <-s.heartbeat.timeout.C:
				if s.state == leader {
					err = s.sendHeartbeat()
				} else {
					err = s.startElection()
				}
			}
		}
		if err != nil {
			return
		}
	}()
	return err
}

func (s *RaftServer) State() State {
	return s.state
}

func (s *RaftServer) gracefulShutdown() {
	fmt.Printf("\nStopping server on on %v\n", s.address)
	s.srv.Stop()
	os.Exit(1)
}

func (s *RaftServer) sendHeartbeat() (err error) {
	var stream api.Raft_AppendEntriesClient
	in := &api.AppendEntriesRequest{}
	var out *api.AppendEntriesReply

	for _, peer := range s.quorum {
		stream, err = peer.Client.AppendEntries(context.TODO())
		stream.Send(in)
		if out, err = stream.CloseAndRecv(); err != nil {
			return err
		}
	}
	return nil
}

func (s *RaftServer) startElection() (err error) {
	s.currentTerm++

	var out *api.VoteReply
	in := &api.VoteRequest{
		Term:         s.currentTerm,
		CandidateId:  s.id,
		LastLogIndex: s.lastApplied,
		LastLogTerm:  s.log[s.lastApplied].Term,
	}

	votesReceived := 0
	votesNeeded := len(s.quorum) + 1

	for _, peer := range s.quorum {

		if out, err = peer.Client.RequestVote(context.TODO(), in); err != nil {
			return err
		}

		if out.VoteGranted {

			votesReceived++
			if votesReceived >= votesNeeded {

				s.state = leader

				if err = s.initialize(true); err != nil {
					return err
				}

				if err = s.sendHeartbeat(); err != nil {
					return err
				}
			}
		}
	}
	return nil
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
		if err == io.EOF {
			log.Info().Msg(fmt.Sprintf("current log: %v", s.log))
			return stream.SendAndClose(
				&api.AppendEntriesReply{
					Success: true,
				},
			)
		}
		if err != nil {
			return err
		}
		s.log = append(s.log, req.Entries...)

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
	}
}

// Invoked by candidates to gather votes (§5.2).
//
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1).
// 2. If votedFor is null or candidateId, and candidate’s log is at
//    least as up-to-date as receiver’s log, grant vote (sections §5.2, §5.4 of the
//    Raft whitepaper).
// TODO: implement for multi-replica version with leader election
func (s *RaftServer) RequestVote(ctx context.Context, req *api.VoteRequest) (rep *api.VoteReply, err error) {
	return &api.VoteReply{}, nil
}

func (s *RaftServer) initialize(electedLeader bool) error {
	if !electedLeader {
		s.currentTerm = 0
		s.votedFor = ""
		s.commitIndex = 0
		s.lastApplied = 0
		s.state = follower
		s.heartbeat = NewTicker(ElectionTimeoutTick)
		err := s.findQuorum()
		return err
	} else {
		s.nextIndex = int64(len(s.log) + 1)
		s.matchIndex = 0
		s.state = leader
		s.heartbeat = NewTicker(HeartbeatTick)
		return nil
	}
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
