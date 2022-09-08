package raft

import (
	"Raft/api"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/rs/zerolog"
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
	ElectionTimeoutTick time.Duration = time.Second * 5
	HeartbeatTick       time.Duration = time.Second * 1
)

type Ticker struct {
	period  time.Duration
	timeout *time.Ticker
	ch      <-chan time.Time
}

func (t *Ticker) Reset(period time.Duration) {
	t.timeout.Reset(period)
}

func NewTicker(period time.Duration) *Ticker {
	timeout := *time.NewTicker(period)
	return &Ticker{period: period, timeout: &timeout, ch: timeout.C}
}

type RaftServer struct {
	sync.Mutex
	api.UnimplementedRaftServer

	// channels
	timeout   *Ticker
	heartbeat chan bool
	shutdown  chan os.Signal
	errC      chan error

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
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Create a new gRPC Raft server.
	raftNode := RaftServer{srv: grpc.NewServer()}
	raftNode.id = id
	raftNode.port = port
	if err = raftNode.initialize(false); err != nil {
		return err
	}
	api.RegisterRaftServer(raftNode.srv, &raftNode)

	// Create a channel to receive interrupt signals and shutdown the server.
	raftNode.shutdown = make(chan os.Signal, 2)
	signal.Notify(raftNode.shutdown, os.Interrupt)
	raftNode.heartbeat = make(chan bool, 1)
	raftNode.address = fmt.Sprintf("localhost:%d", port)

	// Create a socket on the specified port.
	var listener net.Listener
	if listener, err = net.Listen("tcp", raftNode.address); err != nil {
		log.Error().Msg(err.Error())
		return err
	}

	// Start the oneBigPipe event loop and serve in a go function with the created socket.
	fmt.Printf("serving %v on %v\n", id, raftNode.address)
	fmt.Printf("node's quorum: %v\n", raftNode.quorum)
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
	prevState := s.state
	go func() {
		for {
			select {
			case <-s.shutdown:
				s.gracefulShutdown()
				return
			case <-s.timeout.ch:
				if s.state == leader {
					s.sendHeartbeat()
					s.timeout.Reset(HeartbeatTick)
				} else {
					s.startElection()
					go s.conductElection()
					heartbeatReceived := <-s.heartbeat
					if heartbeatReceived {
						fmt.Println("heartbeat received from new leader, reverting to follower")
						s.becomeFollower()
						s.timeout.Reset(HeartbeatTick)
					} else {
						var err error
						if err = s.becomeLeader(); err != nil {
							s.errC <- err
							return
						}

						fmt.Println("sending first heartbeat as leader")
						s.sendHeartbeat()

						if err = s.initialize(true); err != nil {
							s.errC <- err
							return
						}
					}
					s.resetVotedFor()
				}
			}
			if s.state != prevState {
				log.Info().Msg(fmt.Sprintf("state changed from %v to %v", States[prevState], States[s.state]))
				prevState = s.state
			}
		}
	}()
}

// Stope the server, log and exit.
func (s *RaftServer) gracefulShutdown() {
	fmt.Printf("Stopping server on %v\n", s.address)
	s.srv.Stop()
	os.Exit(1)
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
		s.timeout.Reset(HeartbeatTick)
	}
	return nil
}

const timeoutJitter = 500

func (s *RaftServer) electionTick() time.Duration {
	//rand.Seed(int64(s.port)) // Use this when you want predictability
	seed := int64(time.Now().Unix()) + int64(s.port)
	rand.Seed(seed)
	electionTimeoutEpsilon := time.Microsecond * time.Duration(rand.Intn(timeoutJitter))
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
	return nil
}

func CreateClient(address string) (client api.RaftClient, err error) {
	fmt.Println("Creating Client")

	opts := grpc.WithTransportCredentials(insecure.NewCredentials())

	var conn *grpc.ClientConn
	if conn, err = grpc.Dial(address, opts); err != nil {
		return nil, err
	}

	client = api.NewRaftClient(conn)
	return client, nil
}
