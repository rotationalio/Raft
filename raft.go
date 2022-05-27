package main

import (
	"Raft/api"
	"context"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"

	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
			Action:   appendValues,
			Flags: []cli.Flag{
				&cli.Int64Flag{
					Name:    "port",
					Aliases: []string{"p"},
					Usage:   "port to serve on",
					Value:   9000,
				},
				&cli.StringFlag{
					Name:    "values",
					Aliases: []string{"v"},
					Usage:   "values to store",
				},
				&cli.IntFlag{
					Name:    "term",
					Aliases: []string{"t"},
					Usage:   "current term",
					Value:   1,
				},
			},
		},
	}
	app.Run(os.Args)
}

type RaftServer struct {
	api.UnimplementedRaftServer
	CurrentTerm int32
	Id          string
	Log         []*api.Entry
	CommitIndex int64
}

func serve(c *cli.Context) (err error) {
	// Create a new gRPC Raft server
	srv := grpc.NewServer()
	api.RegisterRaftServer(srv, RaftServer{})

	// Create a channel to receive interupt signals and shutdown the server.
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt)
	address := fmt.Sprintf("localhost:%d", c.Int("port"))
	go func() {
		<-ch
		fmt.Printf("Stopping server on on %v", address)
		srv.Stop()
		os.Exit(1)
	}()

	// Create a socket on the specified port.
	var sock net.Listener
	if sock, err = net.Listen("tcp", address); err != nil {
		log.Error().Msg(err.Error())
		return err
	}

	// serve in a go function with the created socket.
	log.Info().Msg(fmt.Sprintf("servering on %v", address))
	srv.Serve(sock)
	return nil
	// TODO: Handle timeouts, votes, etc...
}

func appendValues(c *cli.Context) (err error) {
	client, err := createClient(c.Int("port"))
	if err != nil {
		log.Error().Msg(fmt.Sprintf("error creating client: %v", err.Error()))
		return err
	}

	// Call AppendEntries and print the reply
	vals := strings.Split(c.String("values"), ",")

	var stream api.Raft_AppendEntriesClient
	if stream, err = client.AppendEntries(context.Background()); err != nil {
		log.Error().Msg(fmt.Sprintf("error creating stream: %v", err.Error()))
		return err
	}

	for _, val := range vals {
		req := &api.AppendEntriesRequest{
			Term: int32(c.Int("term")),
			Entries: []*api.Entry{
				{
					Term:  int32(c.Int("term")),
					Value: val,
				},
			},
		}
		log.Info().Msg(fmt.Sprintf("appending %v", val))
		stream.Send(req)
	}
	var reply *api.AppendEntriesReply
	if reply, err = stream.CloseAndRecv(); err != nil {
		log.Error().Msg(fmt.Sprintf("error finishing stream: %v", err.Error()))
		return err
	}
	log.Info().Msg(fmt.Sprintf("success: %t", reply.Success))
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
func (s RaftServer) AppendEntries(stream api.Raft_AppendEntriesServer) (err error) {
	var req *api.AppendEntriesRequest
	for {
		req, err = stream.Recv()
		if err == io.EOF {
			log.Info().Msg(fmt.Sprintf("current log: %v", s.Log))
			return stream.SendAndClose(
				&api.AppendEntriesReply{
					Success: true,
				},
			)
		}
		if err != nil {
			return err
		}
		s.Log = append(s.Log, req.Entries...)
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
// TODO: To impliment after single-replica version
func RequestVote(ctx context.Context, req *api.VoteRequest) (rep *api.VoteReply, err error) {
	return &api.VoteReply{}, nil
}

func createClient(port int) (client api.RaftClient, err error) {
	log.Info().Msg("Creating Client")
	var conn *grpc.ClientConn
	if conn, err = grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		return nil, err
	}
	client = api.NewRaftClient(conn)
	return client, nil
}
