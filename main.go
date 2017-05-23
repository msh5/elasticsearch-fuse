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
	// Attribute of root directory
	if name == "" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
	}

	// Attribute of the directories for index
	nameElems := strings.Split(name, "/")
	if len(nameElems) == 1 {
		exists := false
		for _, indexName := range fs.indexNames {
			if indexName == nameElems[0] {
				exists = true
				break
			}
		}
		if exists {
			return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
		}
	}

	// Attributes of the directory for document type
	if len(nameElems) == 2 {
		// Classify the 'nameElems' elements
		indexName := nameElems[0]
		docType := nameElems[1]

		// Return the directory attribute if the type exists
		mappingsByIndex, ok := fs.mappings[indexName]
		if ok {
			_, ok := mappingsByIndex.(map[string]interface{})["mappings"].(map[string]interface{})[docType]
			if ok {
				return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
			}
		}
	}

	// Attributes of the file for document
	if len(nameElems) == 3 {
		// Classify the 'nameElems' elements
		indexName := nameElems[0]
		docType := nameElems[1]
		docID := nameElems[2]

		// Return the file attribute if the document exists
		docsByIndex, ok := fs.documents[indexName]
		if ok {
			docsByType, ok := docsByIndex[docType]
			if ok {
				docSource, ok := docsByType[docID]
				if ok {
					fileSize := uint64(len(docSource))
					return &fuse.Attr{Mode: fuse.S_IFREG | 0444, Size: fileSize}, fuse.OK
				}
			}
		}
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

func (fs *elasticSearchFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, st fuse.Status) {
	nameElems := strings.Split(name, "/")
	if len(nameElems) == 3 {
		docsByIndex := fs.documents[nameElems[0]]
		if docsByIndex != nil {
			docsByType := docsByIndex[nameElems[1]]
			if docsByType != nil {
				docSource := docsByType[nameElems[2]]
				if docSource != nil {
					return nodefs.NewDataFile(docSource), fuse.OK
				}
			}
		}
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
		docsByIndex := make(map[string]map[string][]byte)
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
			docsByIndex[docType] = documentsByDocType
		}
		documents[indexName] = docsByIndex
	}
	fs := pathfs.NewPathNodeFs(&elasticSearchFs{pathfs.NewDefaultFileSystem(), indexNames, mappings, documents}, nil)

	// Start the FUSE server
	fuseServer, _, err := nodefs.MountRoot(mountPath, fs.Root(), nil)
	if err != nil {
		log.Fatalf("Failed to mount root: error=%v¥n", err)
	}
	fuseServer.Serve()
}
