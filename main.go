package main

import (
	"context"
	"flag"
	"log"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	elastic "gopkg.in/olivere/elastic.v5"
)

type elasticSearchFs struct {
	pathfs.FileSystem

	indexNames []string
	mappings   map[string]interface{}
}

func (fs *elasticSearchFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" || name == "foo" || name == "bar" || name == "foo/test" || name == "bar/test2" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *elasticSearchFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	if name == "" {
		for _, indexName := range fs.indexNames {
			c = append(c, fuse.DirEntry{Name: indexName, Mode: fuse.S_IFDIR})
		}
		return c, fuse.OK
	}
	if name == "foo" || name == "bar" {
		indexMappings := fs.mappings[name].(map[string]interface{})["mappings"].(map[string]interface{})
		for docType := range indexMappings {
			c = append(c, fuse.DirEntry{Name: docType, Mode: fuse.S_IFDIR})
		}
		return c, fuse.OK
	}
	return nil, fuse.ENOENT
}

func main() {
	// Parse the command arguments
	flag.Parse()
	if len(flag.Args()) < 2 {
		log.Fatal("Usage: elasticsearch-fuse ELASTICSEARCH_URL MOUNT_PATH")
	}
	dbURL := flag.Arg(0)
	mountPath := flag.Arg(1)

	// Create the filesystem is specialized for Elasticsearch
	dbClient, err := elastic.NewClient(elastic.SetURL(dbURL))
	if err != nil {
		log.Fatalf("Failed to new client: error=%v¥n", err)
	}
	indexNames, err := dbClient.IndexNames()
	if err != nil {
		log.Fatalf("Failed to fetch index names: error=%v¥n", err)
	}
	mappings, err := dbClient.GetMapping().Do(context.Background())
	if err != nil {
		log.Fatalf("Failed to fetch document types: error=%v¥n", err)
	}
	fs := pathfs.NewPathNodeFs(&elasticSearchFs{pathfs.NewDefaultFileSystem(), indexNames, mappings}, nil)

	// Start the FUSE server
	fuseServer, _, err := nodefs.MountRoot(mountPath, fs.Root(), nil)
	if err != nil {
		log.Fatalf("Failed to mount root: error=%v¥n", err)
	}
	fuseServer.Serve()
}
