package main

import (
	"Raft/api"
	"context"
	"net"
	"os"
	"os/signal"

	"fmt"

	"google.golang.org/grpc"
)

type server struct {
	api.UnimplementedRaftServer
}

func main() {
	fmt.Printf("foo")
}

func Serve(port int) (err error) {
	// Create a new gRPC Raft server
	srv := grpc.NewServer()
	api.RegisterRaftServer(srv, &server{})

	// Create a channel to receive interupt signals and shutdown the server.
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		fmt.Printf("\nStopping server on localhost:%d\n", port)
		srv.Stop()
		os.Exit(1)
	}()

	// Create a socket on the specified port.
	var sock net.Listener
	if sock, err = net.Listen("tcp", fmt.Sprintf("localhost:%d", port)); err != nil {
		return err
	}

	// serve in a go function with the created socket.
	go func() {
		srv.Serve(sock)
	}()
	return nil
}

// Invoked by leader to replicate log entries (section 5.3 of the Raft whitepaper);
// also used as heartbeat (section 5.2 of the Raft whitepaper).
//
// Receiver implementation:
// 1. Reply false if term < currentTerm (section §5.1 of the Raft whitepaper)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
//    whose term matches prevLogTerm (section §5.3 of the Raft whitepaper)
// 3. If an existing entry conflicts with a new one (same index
//    but different terms), delete the existing entry and all that
//    follow it (section §5.3 of the Raft whitepaper)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex =
//    min(leaderCommit, index of last new entry)
func AppendEntries(ctx context.Context, req *api.AppendEntriesRequest) (rep *api.AppendEntriesReply, err error) {
	return &api.AppendEntriesReply{}, nil
}

// Invoked by candidates to gather votes (§5.2).
//
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1).
// 2. If votedFor is null or candidateId, and candidate’s log is at
//    least as up-to-date as receiver’s log, grant vote (sections §5.2, §5.4 of the
//    Raft whitepaper).
func RequestVote(ctx context.Context, req *api.VoteRequest) (rep *api.VoteReply, err error) {
	return &api.VoteReply{}, nil
}
