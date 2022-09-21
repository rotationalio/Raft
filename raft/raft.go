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

type RaftServer struct {
	sync.Mutex
	api.UnimplementedRaftServer

	// channels
	shutdown chan os.Signal
	errC     chan error

	// Self identification
	id            string // The uuid uniquely identifying the raft node.
	port          int    // The port the node is serving on.
	address       string
	srv           *grpc.Server
	quorum        []Peer
	lastHeartbeat time.Time

	// Persistent state on all servers (Updated on stable storage before responding to RPCs).
	currentTerm int32        // latest term server has seen (initialized to 0 on first boot, increases monotonically).
	votedFor    string       // candidate Id that received vote in current term (or nil if none).
	log         []*api.Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1).

	// Volatile state on all servers
	//commitIndex int64 // index of the highest log entry known to be committed (initialized to 0, increases monotonically).
	lastApplied int64 // index of highest log entry applied to state machine (initialized to 0, increases monotonically).
	state       State // The current state of the node, either leader, candidate or follower.

	// Volatile state on leaders (Reinitialized after election).
	// TODO: change these to slices when dealing with multiple replica state
	//nextIndex  int64 // for each server, index of the next log entry to send to that server (initialized to leader's last log index + 1).
	//matchIndex int64 // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically).
}

func ServeRaft(port int, id string) (err error) {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Create a new gRPC Raft server.
	raftNode := RaftServer{srv: grpc.NewServer()}
	raftNode.id = id
	raftNode.port = port
	raftNode.becomeFollower(0)
	api.RegisterRaftServer(raftNode.srv, &raftNode)

	// Create a channel to receive interrupt signals and shutdown the server.
	raftNode.errC = make(chan error, 1)
	raftNode.shutdown = make(chan os.Signal, 2)
	signal.Notify(raftNode.shutdown, os.Interrupt)
	raftNode.address = fmt.Sprintf("localhost:%d", port)

	// Create a socket on the specified port.
	var listener net.Listener
	if listener, err = net.Listen("tcp", raftNode.address); err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	rand.Seed(int64(port))

	// Start election timer and serve with the created socket.
	if err = raftNode.findQuorum(); err != nil {
		return err
	}
	fmt.Printf("node's quorum: %v\n", raftNode.quorum)

	raftNode.lastHeartbeat = time.Now()
	raftNode.runElectionTimer()

	fmt.Printf("serving %v on %v\n", id, raftNode.address)
	err = raftNode.Serve(listener)
	return err
}

func (s *RaftServer) Serve(listener net.Listener) error {
	go func() {
		err := <-s.errC
		fmt.Printf("encountered error: %v\n", err)
		s.gracefulShutdown()
	}()
	err := s.srv.Serve(listener)
	return err
}

// Stope the server, log and exit.
func (s *RaftServer) gracefulShutdown() {
	fmt.Printf("Stopping server on %v\n", s.address)
	s.srv.Stop()
	os.Exit(1)
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
