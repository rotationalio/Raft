package main

import (
	"Raft/api"
	"Raft/raft"
	"context"
	"fmt"
	"os"
	"strings"

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
					Name:     "port",
					Aliases:  []string{"p"},
					Usage:    "port to serve on",
					Required: true,
				},
				&cli.StringFlag{
					Name:     "id",
					Aliases:  []string{"i"},
					Usage:    "name of the node",
					Required: true,
				},
			},
		},
		{
			Name:     "submit",
			Usage:    "",
			Category: "Client",
			Action:   SubmitValues,
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

func serve(c *cli.Context) (err error) {
	if err = raft.ServeRaft(c.Int("port"), c.String("id")); err != nil {
		return cli.Exit(err, 1)
	}
	return nil
}

// Call AppendEntries and print the reply
func SubmitValues(c *cli.Context) (err error) {
	//
	var client api.RaftClient
	address := fmt.Sprintf("localhost:%d", c.Int("port"))
	if client, err = raft.CreateClient(address); err != nil {
		log.Error().Msg(fmt.Sprintf("error creating raft client: %v", err.Error()))
		return cli.Exit(err, 1)
	}

	//
	vals := strings.Split(c.String("values"), ",")
	for _, val := range vals {
		log.Info().Msg(fmt.Sprintf("appending %v", val))
		req := &api.SubmitRequest{Value: []byte(val)}
		var rep *api.SubmitReply
		if rep, err = client.Submit(context.Background(), req); err != nil {
			log.Error().Msg(fmt.Sprintf("error sending on stream: %v", err.Error()))
			return cli.Exit(err, 1)
		}
		log.Info().Msg(fmt.Sprintf("success: %t", rep.Success))
	}
	return nil
}
