package main

import (
	"context"
	"flag"
	"log"
	"strings"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	elastic "gopkg.in/olivere/elastic.v5"
)

type elasticSearchFs struct {
	pathfs.FileSystem

	indexNames []string
	mappings   map[string]interface{}
	documents  map[string]map[string]map[string][]byte
}

func (fs *elasticSearchFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" || name == "foo" || name == "bar" || name == "foo/test" || name == "foo/bar" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
	}
	if strings.HasPrefix(name, "foo/test/") || strings.HasPrefix(name, "foo/bar/") {
		return &fuse.Attr{Mode: fuse.S_IFREG | 0444}, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *elasticSearchFs) OpenDir(name string, context *fuse.Context) (entries []fuse.DirEntry, st fuse.Status) {
	if name == "" {
		for _, indexName := range fs.indexNames {
			entries = append(entries, fuse.DirEntry{Name: indexName, Mode: fuse.S_IFDIR})
		}
		return entries, fuse.OK
	}
	if name == "foo" || name == "bar" {
		indexMappings := fs.mappings[name].(map[string]interface{})["mappings"].(map[string]interface{})
		for docType := range indexMappings {
			entries = append(entries, fuse.DirEntry{Name: docType, Mode: fuse.S_IFDIR})
		}
		return entries, fuse.OK
	}
	if name == "foo/test" {
		for docID := range fs.documents["foo"]["test"] {
			entries = append(entries, fuse.DirEntry{Name: docID, Mode: fuse.S_IFREG})
		}
		return entries, fuse.OK
	}
	if name == "bar/test2" {
		for docID := range fs.documents["bar"]["test2"] {
			entries = append(entries, fuse.DirEntry{Name: docID, Mode: fuse.S_IFREG})
		}
		return entries, fuse.OK
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
	documents := make(map[string]map[string]map[string][]byte)
	for indexName, mappingsByIndex := range mappings {
		indexMappings := mappingsByIndex.(map[string]interface{})["mappings"].(map[string]interface{})
		documentsByIndex := make(map[string]map[string][]byte)
		for docType := range indexMappings {
			documentsByDocType := make(map[string][]byte)
			result, err2 := dbClient.Search().Index(indexName).Type(docType).Do(context.Background())
			if err2 != nil {
				log.Fatalf("Failed to fetch documents: error=%v¥n", err2)
			}
			for _, hit := range result.Hits.Hits {
				docSource, err2 := hit.Source.MarshalJSON()
				if err2 != nil {
					log.Fatalf("Failed to fetch documents: error=%v¥n", err2)
				}
				documentsByDocType[hit.Id] = docSource
			}
			documentsByIndex[docType] = documentsByDocType
		}
		documents[indexName] = documentsByIndex
	}
	fs := pathfs.NewPathNodeFs(&elasticSearchFs{pathfs.NewDefaultFileSystem(), indexNames, mappings, documents}, nil)

	// Start the FUSE server
	fuseServer, _, err := nodefs.MountRoot(mountPath, fs.Root(), nil)
	if err != nil {
		log.Fatalf("Failed to mount root: error=%v¥n", err)
	}
	fuseServer.Serve()
}
