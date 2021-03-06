package main

import (
	"Raft/api"
	"context"
	"os"
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
			Before:   createClient,
			Flags: []cli.Flag{
				&cli.Int64Flag{
					Name:    "port",
					Aliases: []string{"p"},
					Usage:   "port to dial",
					Value:   9000,
				},
				&cli.StringFlag{
					Name:    "values",
					Aliases: []string{"v"},
					Usage:   "values to store",
				},
				// Currently for testing purposes
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

var (
	client api.RaftClient
)

func serve(c *cli.Context) (err error) {
	if err = serveRaft(c.Int("port")); err != nil {
		return cli.Exit(err, 1)
	}
	return nil
}

// Call AppendEntries and print the reply
func appendValues(c *cli.Context) (err error) {
	var stream api.Raft_AppendEntriesClient
	if stream, err = client.AppendEntries(context.Background()); err != nil {
		log.Error().Msg(fmt.Sprintf("error creating stream: %v", err.Error()))
		return cli.Exit(err, 1)
	}

	vals := strings.Split(c.String("values"), ",")
	for _, val := range vals {
		log.Info().Msg(fmt.Sprintf("appending %v", val))
		req := createAppendRequest(int32(c.Int("term")), val)
		if err = stream.Send(req); err != nil {
			log.Error().Msg(fmt.Sprintf("error sending on stream: %v", err.Error()))
			return cli.Exit(err, 1)
		}
	}

	var reply *api.AppendEntriesReply
	if reply, err = stream.CloseAndRecv(); err != nil {
		log.Error().Msg(fmt.Sprintf("error finishing stream: %v", err.Error()))
		return cli.Exit(err, 1)
	}
	log.Info().Msg(fmt.Sprintf("success: %t", reply.Success))
	return nil
}

func createAppendRequest(term int32, val string) *api.AppendEntriesRequest {
	return &api.AppendEntriesRequest{
		Term: term,
		Entries: []*api.Entry{
			{
				Term:  term,
				Value: val,
			},
		},
	}
}

func createClient(c *cli.Context) (err error) {
	log.Info().Msg("Creating Client")
	var conn *grpc.ClientConn
	opts := grpc.WithTransportCredentials(insecure.NewCredentials())
	if conn, err = grpc.Dial(fmt.Sprintf("localhost:%d", c.Int("port")), opts); err != nil {
		return err
	}
	client = api.NewRaftClient(conn)
	return nil
}
