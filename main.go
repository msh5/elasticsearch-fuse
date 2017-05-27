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

const appName = "Elasticsearch-FUSE"
const appVersion = "0.1.0"

type elasticSearchFs struct {
	pathfs.FileSystem

	indexNames []string
	mappings   map[string]interface{}
	documents  map[string]map[string]map[string][]byte
}

func (fs *elasticSearchFs) findMappingsByIndex(indexName string) (map[string]interface{}, bool) {
	mappings, ok := fs.mappings[indexName]
	if ok {
		return mappings.(map[string]interface{})["mappings"].(map[string]interface{}), true
	}
	return nil, false
}

func (fs *elasticSearchFs) findMapping(indexName string, docType string) (interface{}, bool) {
	mappingsByIndex, ok := fs.findMappingsByIndex(indexName)
	if ok {
		mapping, ok := mappingsByIndex[docType]
		if ok {
			return mapping, true
		}
	}
	return nil, false
}

func (fs *elasticSearchFs) findDocumentsByType(indexName string, docType string) (map[string][]byte, bool) {
	docsByIndex, ok := fs.documents[indexName]
	if ok {
		docsByType, ok := docsByIndex[docType]
		if ok {
			return docsByType, true
		}
	}
	return nil, false
}

func (fs *elasticSearchFs) findDocument(indexName string, docType string, docID string) ([]byte, bool) {
	docsByType, ok := fs.findDocumentsByType(indexName, docType)
	if ok {
		docSource, ok := docsByType[docID]
		if ok {
			return docSource, true
		}
	}
	return nil, false
}

func (fs *elasticSearchFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	// Return the attribute of the root directory
	if name == "" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
	}

	// Return the attribute of the index directory
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

	// Return the attributes of the document type directory
	if len(nameElems) == 2 {
		_, ok := fs.findMapping(nameElems[0], nameElems[1])
		if ok {
			return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
		}
	}

	// Return the attributes of the document file
	if len(nameElems) == 3 {
		docSource, ok := fs.findDocument(nameElems[0], nameElems[1], nameElems[2])
		if ok {
			fileSize := uint64(len(docSource))
			return &fuse.Attr{Mode: fuse.S_IFREG | 0444, Size: fileSize}, fuse.OK
		}
	}
	return nil, fuse.ENOENT
}

func (fs *elasticSearchFs) OpenDir(name string, context *fuse.Context) (entries []fuse.DirEntry, st fuse.Status) {
	// If the root directory is opened, list up index names as the directory entries.
	if name == "" {
		for _, indexName := range fs.indexNames {
			entries = append(entries, fuse.DirEntry{Name: indexName, Mode: fuse.S_IFDIR})
		}
		return entries, fuse.OK
	}

	// If the index directory is opened, list up document types as the directory entries.
	nameElems := strings.Split(name, "/")
	if len(nameElems) == 1 {
		mappingsByIndex, ok := fs.findMappingsByIndex(nameElems[0])
		if ok {
			for docType := range mappingsByIndex {
				entries = append(entries, fuse.DirEntry{Name: docType, Mode: fuse.S_IFDIR})
			}
			return entries, fuse.OK
		}
	}

	// If the document type directory is opened, list up documents as the file entries.
	if len(nameElems) == 2 {
		documents, ok := fs.findDocumentsByType(nameElems[0], nameElems[1])
		if ok {
			for docID := range documents {
				entries = append(entries, fuse.DirEntry{Name: docID, Mode: fuse.S_IFREG})
			}
			return entries, fuse.OK
		}
	}
	return nil, fuse.ENOENT
}

func (fs *elasticSearchFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, st fuse.Status) {
	nameElems := strings.Split(name, "/")
	if len(nameElems) == 3 {
		docSource, ok := fs.findDocument(nameElems[0], nameElems[1], nameElems[2])
		if ok {
			return nodefs.NewDataFile(docSource), fuse.OK
		}
	}
	return nil, fuse.ENOENT
}

func main() {
	// Parse the command arguments
	dbURL := flag.String("db", "http://localhost:9200", "Elasticsearch URL to connect")
	mountPath := flag.String("mp", "./elasticsearch-fuse", "Directory path as mount point")
	versionMode := flag.Bool("version", false, "Switch mode into version reporting")
	flag.Parse()

	// IF version arg is specified, report the app version and exit immediately.
	if *versionMode {
		log.Printf("%s %s\n", appName, appVersion)
		return
	}

	// Create the filesystem is specialized for Elasticsearch
	dbClient, err := elastic.NewClient(elastic.SetURL(*dbURL))
	if err != nil {
		log.Fatalf("Failed to new client: error=%v\n", err)
	}
	indexNames, err := dbClient.IndexNames()
	if err != nil {
		log.Fatalf("Failed to fetch index names: error=%v\n", err)
	}
	mappings, err := dbClient.GetMapping().Do(context.Background())
	if err != nil {
		log.Fatalf("Failed to fetch document types: error=%v\n", err)
	}
	documents := make(map[string]map[string]map[string][]byte)
	for indexName, mappingsByIndex := range mappings {
		indexMappings := mappingsByIndex.(map[string]interface{})["mappings"].(map[string]interface{})
		docsByIndex := make(map[string]map[string][]byte)
		for docType := range indexMappings {
			docsByType := make(map[string][]byte)
			result, err2 := dbClient.Search().Index(indexName).Type(docType).Size(-1).Do(context.Background())
			if err2 != nil {
				log.Fatalf("Failed to fetch documents: error=%v\n", err2)
			}
			for _, hit := range result.Hits.Hits {
				docSource, err2 := hit.Source.MarshalJSON()
				if err2 != nil {
					log.Fatalf("Failed to fetch documents: error=%v\n", err2)
				}
				docsByType[hit.Id] = docSource
			}
			docsByIndex[docType] = docsByType
		}
		documents[indexName] = docsByIndex
	}
	fs := pathfs.NewPathNodeFs(&elasticSearchFs{pathfs.NewDefaultFileSystem(), indexNames, mappings, documents}, nil)

	// Start the FUSE server
	fuseServer, _, err := nodefs.MountRoot(*mountPath, fs.Root(), nil)
	if err != nil {
		log.Fatalf("Failed to mount root: error=%v\n", err)
	}
	fuseServer.Serve()
}
