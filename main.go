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

	debugMode bool
	documents map[string]map[string]map[string][]byte
}

func fetchIndexNames(db *elastic.Client) []string {
	indexNames, err := db.IndexNames()
	if err != nil {
		log.Fatalf("Failed to fetch index names: error=%v\n", err)
	}
	return indexNames
}

func fetchDocumentTypes(db *elastic.Client, indexName string) []string {
	mappings, err := db.GetMapping().Do(context.Background())
	if err != nil {
		log.Fatalf("Failed to fetch document types: error=%v\n", err)
	}
	for curIndexName, curMappings := range mappings {
		if curIndexName == indexName {
			mappingsByIndex := curMappings.(map[string]interface{})["mappings"].(map[string]interface{})
			docTypes := make([]string, 0)
			for docType := range mappingsByIndex {
				docTypes = append(docTypes, docType)
			}
			return docTypes
		}
	}
	log.Fatalf("Not found: index=%v\n", indexName)
	return nil
}

func fetchDocuments(db *elastic.Client, indexName string, docType string) map[string][]byte {
	docs := make(map[string][]byte)
	result, err := db.Search().Index(indexName).Type(docType).Size(-1).Do(context.Background())
	if err != nil {
		log.Fatalf("Failed to fetch documents: error=%v\n", err)
	}
	for _, hit := range result.Hits.Hits {
		docSource, err := hit.Source.MarshalJSON()
		if err != nil {
			log.Fatalf("Failed to fetch documents: error=%v\n", err)
		}
		docs[hit.Id] = docSource
	}
	return docs
}

func (self *elasticSearchFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if self.debugMode {
		log.Printf("GetAttr: name=%v\n", name)
	}

	// Return the attribute of the root directory
	if name == "" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
	}

	// Return the attribute of the index directory
	nameElems := strings.Split(name, "/")
	if len(nameElems) == 1 {
		_, ok := self.documents[nameElems[0]]
		if ok {
			return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
		}
	}

	// Return the attributes of the document type directory
	if len(nameElems) == 2 {
		docsByIndex, ok := self.documents[nameElems[0]]
		if ok {
			_, ok := docsByIndex[nameElems[1]]
			if ok {
				return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
			}
		}
	}

	// Return the attributes of the document file
	if len(nameElems) == 3 {
		docsByIndex, ok := self.documents[nameElems[0]]
		if ok {
			docsByType, ok := docsByIndex[nameElems[1]]
			if ok {
				docSource, ok := docsByType[nameElems[2]]
				if ok {
					fileSize := uint64(len(docSource))
					return &fuse.Attr{Mode: fuse.S_IFREG | 0444, Size: fileSize}, fuse.OK
				}
			}
		}
	}
	return nil, fuse.ENOENT
}

func (self *elasticSearchFs) OpenDir(name string, context *fuse.Context) (entries []fuse.DirEntry, st fuse.Status) {
	if self.debugMode {
		log.Printf("OpenDir: name=%v\n", name)
	}

	// If the root directory is opened, list up index names as the directory entries.
	if name == "" {
		for indexName := range self.documents {
			entries = append(entries, fuse.DirEntry{Name: indexName, Mode: fuse.S_IFDIR})
		}
		return entries, fuse.OK
	}

	// If the index directory is opened, list up document types as the directory entries.
	nameElems := strings.Split(name, "/")
	if len(nameElems) == 1 {
		docsByIndex, ok := self.documents[nameElems[0]]
		if ok {
			for docType := range docsByIndex {
				entries = append(entries, fuse.DirEntry{Name: docType, Mode: fuse.S_IFDIR})
			}
			return entries, fuse.OK
		}
	}

	// If the document type directory is opened, list up documents as the file entries.
	if len(nameElems) == 2 {
		docsByIndex, ok := self.documents[nameElems[0]]
		if ok {
			docsByType, ok := docsByIndex[nameElems[1]]
			if ok {
				for docID := range docsByType {
					entries = append(entries, fuse.DirEntry{Name: docID, Mode: fuse.S_IFREG})
				}
				return entries, fuse.OK
			}
		}
	}
	return nil, fuse.ENOENT
}

func (self *elasticSearchFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, st fuse.Status) {
	if self.debugMode {
		log.Printf("Open: name=%v, flags=%x\n", name, flags)
	}

	nameElems := strings.Split(name, "/")
	if len(nameElems) == 3 {
		docsByIndex, ok := self.documents[nameElems[0]]
		if ok {
			docsByType, ok := docsByIndex[nameElems[1]]
			if ok {
				docSource, ok := docsByType[nameElems[2]]
				if ok {
					return nodefs.NewDataFile(docSource), fuse.OK
				}
			}
		}
	}
	return nil, fuse.ENOENT
}

func main() {
	// Parse the command arguments
	dbURLs := flag.String("db", "http://localhost:9200", "Elasticsearch URLs to connect")
	mountPath := flag.String("mp", "./elasticsearch-fuse", "Directory path as mount point")
	// TODO: pageSize := flag.String("page", 10, "The number of documents to list in one directory")
	versionMode := flag.Bool("version", false, "Switch mode into version reporting")
	debugMode := flag.Bool("debug", false, "Emit debug logs")
	flag.Parse()

	// IF version arg is specified, report the app version and exit immediately.
	if *versionMode {
		log.Printf("%s %s\n", appName, appVersion)
		return
	}

	// Create the filesystem is specialized for Elasticsearch
	dbURLsAsArray := strings.Split(*dbURLs, ",")
	dbClient, err := elastic.NewClient(elastic.SetURL(dbURLsAsArray...))
	if err != nil {
		log.Fatalf("Failed to new client: error=%v\n", err)
	}
	indexNames := fetchIndexNames(dbClient)
	documents := make(map[string]map[string]map[string][]byte)
	for _, indexName := range indexNames {
		docTypes := fetchDocumentTypes(dbClient, indexName)
		docsByIndex := make(map[string]map[string][]byte)
		for _, docType := range docTypes {
			docsByType := fetchDocuments(dbClient, indexName, docType)
			docsByIndex[docType] = docsByType
		}
		documents[indexName] = docsByIndex
	}
	fs := elasticSearchFs{pathfs.NewDefaultFileSystem(), *debugMode, documents}
	pathNodefs := pathfs.NewPathNodeFs(&fs, nil)

	// Start the FUSE server
	fuseServer, _, err := nodefs.MountRoot(*mountPath, pathNodefs.Root(), nil)
	if err != nil {
		log.Fatalf("Failed to mount root: error=%v\n", err)
	}
	fuseServer.Serve()
}
