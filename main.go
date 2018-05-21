package main

import (
	"flag"
	"log"
)

const appName = "elasticsearch-fuse"
const appVersion = "0.2.0"

func main() {
	// Parse the command arguments
	urls := flag.String("db-urls", "http://localhost:9200", "Elasticsearch URLs to connect")
	mountPath := flag.String("mount-path", "./elasticsearch-fuse", "Directory path as mount point")
	pageSize := flag.Int("page", 10, "The number of documents to list in one directory")
	// TODO: updateInterval := flag.Int("update-interval", 10, "Interval seconds of same queries to Elasticsearch")
	debug := flag.Bool("debug", false, "Emit debug logs")
	version := flag.Bool("version", false, "Switch mode into version reporting")
	flag.Parse()

	// IF version arg is specified, report the app version and exit immediately.
	if *version {
		log.Printf("%s %s\n", appName, appVersion)
		return
	}

	// Create the filesystem is specialized for Elasticsearch
	fs, err := NewElasticsearchFS(*urls, *pageSize, *debug)
	if err != nil {
		log.Fatalf("Failed to new filesystem: error=%v\n", err)
	}

	// Start the FUSE server
	err = MountFilesystem(fs, *mountPath)
	if err != nil {
		log.Fatalf("Failed to mount filesystem: error=%v\n", err)
	}
}
