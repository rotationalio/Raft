package main

import (
	"Raft/api"
	"context"
	"net"
	"os"
	"os/signal"
	"time"

	"fmt"

	"google.golang.org/grpc"

	"github.com/rs/zerolog/log"
	cli "github.com/urfave/cli/v2"
)

func main() {
	app := cli.NewApp()
	app.Name = "Raft"
	app.Usage = "Rotational Lab's implementation of the Raft consensus algorithm"
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "conf",
			Aliases: []string{"f"},
			Usage:   "path to the configuration file",
			Value:   "",
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:     "serve",
			Usage:    "run a Raft server",
			Category: "Server",
			Action:   serve,
			Flags: []cli.Flag{
				&cli.Int64Flag{
					Name:    "port",
					Aliases: []string{"p"},
					Usage:   "port to serve on",
					Value:   9000,
				},
			},
		},
		{
			Name:     "append",
			Usage:    "",
			Category: "Client",
			Action:   appendValue,
			Flags: []cli.Flag{
				&cli.Int64Flag{
					Name:    "port",
					Aliases: []string{"p"},
					Usage:   "port to serve on",
					Value:   9000,
				},
				&cli.StringFlag{
					Name:    "value",
					Aliases: []string{"v"},
					Usage:   "value to store",
				},
			},
		},
	}
	app.Run(os.Args)
}

type server struct {
	api.UnimplementedRaftServer
}

func serve(c *cli.Context) (err error) {
	// Create a new gRPC Raft server
	log.Info().Msg("Creating Server")
	srv := grpc.NewServer()
	api.RegisterRaftServer(srv, &server{})

	// Create a channel to receive interupt signals and shutdown the server.
	log.Info().Msg("Creating Interrupt Channel")
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		fmt.Printf("Stopping server on localhost:%d\n", c.Int("port"))
		srv.Stop()
		os.Exit(1)
	}()

	// Create a socket on the specified port.
	log.Info().Msg("Creating Socket")
	var sock net.Listener
	if sock, err = net.Listen("tcp", fmt.Sprintf("localhost:%d", c.Int("port"))); err != nil {
		log.Error().Msg(err.Error())
		return err
	}

	// serve in a go function with the created socket.
	log.Info().Msg("Servering")
	go func() {
		srv.Serve(sock)
	}()

	// TODO: Handle timeouts, votes, etc...
	for {
		fmt.Print("still serving\n")
		time.Sleep(15 * time.Second)
	}
}

func appendValue(c *cli.Context) (err error) {
	// Create a new Raft client
	log.Info().Msg("Creating Client")
	var conn *grpc.ClientConn
	if conn, err = grpc.Dial(fmt.Sprintf("localhost:%d", c.Int("port")), grpc.WithInsecure()); err != nil {
		return err
	}
	defer conn.Close()
	client := api.NewRaftClient(conn)

	// Call AppendEntries and print the reply
	log.Info().Msg(fmt.Sprintf("Appending %v", c.String("value")))
	val := append([]string{}, c.String("value"))
	req := &api.AppendEntriesRequest{Entries: val}
	var rep *api.AppendEntriesReply
	if rep, err = client.AppendEntries(context.Background(), req); err != nil {
		log.Error().Msg(err.Error())
		return err
	}
	fmt.Print(rep)
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
	return &api.AppendEntriesReply{
		Term:    0,
		Success: true,
		Error:   req.Entries[0]}, nil
}

// Invoked by candidates to gather votes (§5.2).
//
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1).
// 2. If votedFor is null or candidateId, and candidate’s log is at
//    least as up-to-date as receiver’s log, grant vote (sections §5.2, §5.4 of the
//    Raft whitepaper).
// TODO: To impliment after single-replica version
func RequestVote(ctx context.Context, req *api.VoteRequest) (rep *api.VoteReply, err error) {
	return &api.VoteReply{}, nil
}
