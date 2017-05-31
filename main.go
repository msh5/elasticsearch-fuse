package main

import (
	"context"
	"flag"
	"log"
	"strconv"
	"strings"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	elastic "gopkg.in/olivere/elastic.v5"
)

const appName = "elasticsearch-fuse"
const appVersion = "0.2.0"

type elasticSearchFs struct {
	pathfs.FileSystem

	debugMode      bool
	pageSize       int
	db             *elastic.Client
	indexNames     []string
	documentTypes  map[string][]string
	documentTotals map[string]map[string]int64
	documents      map[string]map[string]map[int]map[string][]byte
}

func fetchIndexNames(db *elastic.Client) ([]string, error) {
	indexNames, err := db.IndexNames()
	if err != nil {
		return nil, err
	}
	return indexNames, nil
}

func fetchDocumentTypes(db *elastic.Client, indexName string) ([]string, error) {
	mappings, err := db.GetMapping().Do(context.Background())
	if err != nil {
		return nil, err
	}
	docTypes := make([]string, 0)
	for curIndexName, curMappings := range mappings {
		if curIndexName == indexName {
			mappingsByIndex := curMappings.(map[string]interface{})["mappings"].(map[string]interface{})
			for docType := range mappingsByIndex {
				docTypes = append(docTypes, docType)
			}
			break
		}
	}
	return docTypes, nil
}

func countDocuments(db *elastic.Client, indexName string, docType string) (int64, error) {
	result, err := db.Search().Index(indexName).Type(docType).Size(0).Do(context.Background())
	if err != nil {
		return 0, err
	}
	return result.Hits.TotalHits, nil
}

func fetchDocuments(db *elastic.Client, indexName string, docType string, from int, size int) (map[string][]byte, error) {
	docs := make(map[string][]byte)
	result, err := db.Search().Index(indexName).Type(docType).From(from).Size(size).Do(context.Background())
	if err != nil {
		return nil, err
	}
	for _, hit := range result.Hits.Hits {
		docSource, err := hit.Source.MarshalJSON()
		if err != nil {
			return nil, err
		}
		docs[hit.Id] = docSource
	}
	return docs, nil
}

func (self *elasticSearchFs) EnsureIndexNames() ([]string, error) {
	indexNames, err := fetchIndexNames(self.db)
	if err != nil {
		return nil, err
	}
	self.indexNames = indexNames
	return indexNames, nil
}

func (self *elasticSearchFs) EnsureDocumentTypes(indexName string) ([]string, error) {
	documentTypes, err := fetchDocumentTypes(self.db, indexName)
	if err != nil {
		return nil, err
	}
	if self.documentTypes == nil {
		self.documentTypes = make(map[string][]string)
	}
	self.documentTypes[indexName] = documentTypes
	return documentTypes, nil
}

func (self *elasticSearchFs) EnsureDocumentTotal(indexName string, docType string) (int64, error) {
	total, err := countDocuments(self.db, indexName, docType)
	if err != nil {
		return 0, err
	}
	if self.documentTotals == nil {
		self.documentTotals = make(map[string]map[string]int64)
	}
	_, ok := self.documentTotals[indexName]
	if !ok {
		self.documentTotals[indexName] = make(map[string]int64)
	}
	self.documentTotals[indexName][docType] = total
	return total, nil
}

func (self *elasticSearchFs) EnsureDocuments(indexName string, docType string, page int) (map[string][]byte, error) {
	documents, err := fetchDocuments(self.db, indexName, docType, self.pageSize*page, self.pageSize)
	if err != nil {
		return nil, err
	}
	if self.documents == nil {
		self.documents = make(map[string]map[string]map[int]map[string][]byte)
	}
	_, ok := self.documents[indexName]
	if !ok {
		self.documents[indexName] = make(map[string]map[int]map[string][]byte)
	}
	_, ok = self.documents[indexName][docType]
	if !ok {
		self.documents[indexName][docType] = make(map[int]map[string][]byte)
	}
	self.documents[indexName][docType][page] = documents
	return documents, nil
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
		indexNames, err := self.EnsureIndexNames()
		if err != nil {
			log.Fatalf("Failed to ensure the index names: err=%v\n", err)
		}
		for _, indexName := range indexNames {
			if nameElems[0] == indexName {
				return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
			}
		}
	}

	// Return the attributes of the document type directory
	if len(nameElems) == 2 {
		docTypes, err := self.EnsureDocumentTypes(nameElems[0])
		if err != nil {
			log.Fatalf("Failed to ensure the document types: index=%v, err=%v\n", nameElems[0], err)
		}
		for _, docType := range docTypes {
			if nameElems[1] == docType {
				return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
			}
		}
	}

	// Return the attributes of the paging directory
	if len(nameElems) == 3 {
		total, err := self.EnsureDocumentTotal(nameElems[0], nameElems[1])
		if err != nil {
			log.Fatalf("Failed to ensure the documents: index=%v, doctype=%v, err=%v\n", nameElems[0], nameElems[1], err)
		}
		page, err := strconv.Atoi(nameElems[2])
		if err != nil {
			log.Fatalf("Failed to parse the paging directory name as integer: index=%v, doctype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], nameElems[2], err)
		}
		from := int64(self.pageSize * page)
		if from < total {
			return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
		}
	}

	// Return the attributes of the document file
	if len(nameElems) == 4 {
		page, err := strconv.ParseInt(nameElems[2], 10, 0)
		if err != nil {
			log.Fatalf("Failed to parse the paging directory name as integer: index=%v, doctype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], nameElems[2], err)
		}
		documents, err := self.EnsureDocuments(nameElems[0], nameElems[1], int(page))
		if err != nil {
			log.Fatalf("Failed to ensure the documents: index=%v, doctype=%v, err=%v\n", nameElems[0], nameElems[1], err)
		}
		docSource, ok := documents[nameElems[3]]
		fileSize := uint64(len(docSource))
		if ok {
			return &fuse.Attr{Mode: fuse.S_IFREG | 0444, Size: fileSize}, fuse.OK
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
		indexNames, err := self.EnsureIndexNames()
		if err != nil {
			log.Fatalf("Failed to ensure the index names: err=%v\n", err)
		}
		for _, indexName := range indexNames {
			entries = append(entries, fuse.DirEntry{Name: indexName, Mode: fuse.S_IFDIR})
		}
		return entries, fuse.OK
	}

	// If the index directory is opened, list up document types as the directory entries.
	nameElems := strings.Split(name, "/")
	if len(nameElems) == 1 {
		docTypes, err := self.EnsureDocumentTypes(nameElems[0])
		if err != nil {
			log.Fatalf("Failed to ensure the document types: index=%v, err=%v\n", nameElems[0], err)
		}
		for _, docType := range docTypes {
			entries = append(entries, fuse.DirEntry{Name: docType, Mode: fuse.S_IFDIR})
		}
		return entries, fuse.OK
	}

	// If the document type directory is opened, list up documents as the file entries.
	if len(nameElems) == 2 {
		total, err := self.EnsureDocumentTotal(nameElems[0], nameElems[1])
		if err != nil {
			log.Fatalf("Failed to ensure the documents: index=%v, doctype=%v, err=%v\n", nameElems[0], nameElems[1], err)
		}
		for i := 0; int64(i*self.pageSize) < total; i++ {
			entries = append(entries, fuse.DirEntry{Name: strconv.Itoa(i), Mode: fuse.S_IFREG})
		}
		return entries, fuse.OK
	}

	// If the document type directory is opened, list up documents as the file entries.
	if len(nameElems) == 3 {
		page, err := strconv.Atoi(nameElems[2])
		if err != nil {
			log.Fatalf("Failed to parse the paging directory name as integer: index=%v, doctype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], nameElems[2], err)
		}
		documents, err := self.EnsureDocuments(nameElems[0], nameElems[1], int(page))
		if err != nil {
			log.Fatalf("Failed to ensure the documents: index=%v, doctype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], nameElems[2], err)
		}
		for docID := range documents {
			entries = append(entries, fuse.DirEntry{Name: docID, Mode: fuse.S_IFREG})
		}
		return entries, fuse.OK
	}

	return nil, fuse.ENOENT
}

func (self *elasticSearchFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, st fuse.Status) {
	if self.debugMode {
		log.Printf("Open: name=%v, flags=%x\n", name, flags)
	}

	nameElems := strings.Split(name, "/")
	if len(nameElems) == 4 {
		page, err := strconv.Atoi(nameElems[2])
		if err != nil {
			log.Fatalf("Failed to parse the paging directory name as integer: index=%v, doctype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], nameElems[2], err)
		}
		documents, err := self.EnsureDocuments(nameElems[0], nameElems[1], page)
		if err != nil {
			log.Fatalf("Failed to ensure the documents: index=%v, doctype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], page, err)
		}
		docSource, ok := documents[nameElems[3]]
		if ok {
			return nodefs.NewDataFile(docSource), fuse.OK
		}
	}
	return nil, fuse.ENOENT
}

func main() {
	// Parse the command arguments
	dbURLs := flag.String("db-urls", "http://localhost:9200", "Elasticsearch URLs to connect")
	mountPath := flag.String("mount-path", "./elasticsearch-fuse", "Directory path as mount point")
	pageSize := flag.Int("page", 10, "The number of documents to list in one directory")
	// TODO: updateInterval := flag.Int("update-interval", 10, "Interval seconds of same queries to Elasticsearch")
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
	db, err := elastic.NewClient(elastic.SetURL(dbURLsAsArray...))
	if err != nil {
		log.Fatalf("Failed to new client: error=%v\n", err)
	}
	fs := elasticSearchFs{pathfs.NewDefaultFileSystem(), *debugMode, *pageSize, db,
		nil, nil, nil, nil}
	pathNodefs := pathfs.NewPathNodeFs(&fs, nil)

	// Start the FUSE server
	fuseServer, _, err := nodefs.MountRoot(*mountPath, pathNodefs.Root(), nil)
	if err != nil {
		log.Fatalf("Failed to mount root: error=%v\n", err)
	}
	log.Println("Mounted")
	fuseServer.Serve()
}
