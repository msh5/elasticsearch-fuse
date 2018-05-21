package main

import (
	"log"
	"os"

	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "elasticsearch-fuse"
	app.Version = "0.2.0"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "urls",
			Value: "http://localhost:9200",
			Usage: "Elasticsearch server URLs",
		},
		cli.StringFlag{
			Name:  "mount-path",
			Value: "./elasticsearch-fuse",
			Usage: "Directory path as mount point",
		},
		cli.IntFlag{
			Name:  "page",
			Value: 10,
			Usage: "The number of documents to list in one directory",
		},
		// TODO: updateInterval := flag.Int("update-interval", 10, "Interval seconds of same queries to Elasticsearch")
		cli.BoolFlag{
			Name:  "debug",
			Usage: "Emit debug logs",
		},
	}
	app.Action = func(c *cli.Context) error {
		// Get command options
		urls := c.String("urls")
		mountPath := c.String("mount")
		pageSize := c.Int("page")
		debug := c.Bool("debug")

		// Create the filesystem is specialized for Elasticsearch
		fs, err := NewElasticsearchFS(urls, pageSize, debug)
		if err != nil {
			return err
		}

		// Start the FUSE server
		err = MountFilesystem(fs, mountPath)
		if err != nil {
			return err
		}
		return nil
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
