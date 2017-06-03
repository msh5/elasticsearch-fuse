package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	elastic "gopkg.in/olivere/elastic.v5"
)

const appName = "elasticsearch-fuse"
const appVersion = "0.2.0"

type elasticSearchClient struct {
	raw *elastic.Client
}

func (self elasticSearchClient) SelectIndexNames() ([]string, error) {
	return self.raw.IndexNames()
}

func (self elasticSearchClient) SelectDocumentTypes(indexName string) ([]string, error) {
	mappings, err := self.raw.GetMapping().Do(context.Background())
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

func (self elasticSearchClient) CountDocuments(indexName string, docType string) (int64, error) {
	result, err := self.raw.Search().Index(indexName).Type(docType).Size(0).Do(context.Background())
	if err != nil {
		return 0, err
	}
	return result.Hits.TotalHits, nil
}

func (self elasticSearchClient) SelectDocuments(indexName string, docType string, from int, size int) (map[string][]byte, error) {
	docs := make(map[string][]byte)
	result, err := self.raw.Search().Index(indexName).Type(docType).From(from).Size(size).Do(context.Background())
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

type elasticSearchCache struct {
	client   elasticSearchClient
	pageSize int

	indexNames     []string
	documentTypes  map[string][]string
	documentTotals map[string]map[string]int64
	documents      map[string]map[string]map[int]map[string][]byte
}

func (self *elasticSearchCache) IndexNames() ([]string, error) {
	indexNames, err := self.client.SelectIndexNames()
	if err != nil {
		return nil, err
	}
	self.indexNames = indexNames
	return indexNames, nil
}

func (self *elasticSearchCache) DocumentTypes(indexName string) ([]string, error) {
	documentTypes, err := self.client.SelectDocumentTypes(indexName)
	if err != nil {
		return nil, err
	}
	if self.documentTypes == nil {
		self.documentTypes = make(map[string][]string)
	}
	self.documentTypes[indexName] = documentTypes
	return documentTypes, nil
}

func (self *elasticSearchCache) DocumentTotal(indexName string, docType string) (int64, error) {
	total, err := self.client.CountDocuments(indexName, docType)
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

func (self *elasticSearchCache) Documents(indexName string, docType string, page int) (map[string][]byte, error) {
	documents, err := self.client.SelectDocuments(indexName, docType, self.pageSize*page, self.pageSize)
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

type elasticSearchFs struct {
	pathfs.FileSystem

	debugMode bool
	pageSize  int

	db elasticSearchCache
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
		indexNames, err := self.db.IndexNames()
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
		docTypes, err := self.db.DocumentTypes(nameElems[0])
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
		total, err := self.db.DocumentTotal(nameElems[0], nameElems[1])
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
		documents, err := self.db.Documents(nameElems[0], nameElems[1], int(page))
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
		indexNames, err := self.db.IndexNames()
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
		docTypes, err := self.db.DocumentTypes(nameElems[0])
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
		total, err := self.db.DocumentTotal(nameElems[0], nameElems[1])
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
		documents, err := self.db.Documents(nameElems[0], nameElems[1], int(page))
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
		documents, err := self.db.Documents(nameElems[0], nameElems[1], page)
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
	mountPathPtr := flag.String("mount-path", "{mount-path-dir}/{mount-path-base}", "Directory path as mount point")
	mountPathDir := flag.String("mount-path-dir", ".", "Directory path as mount point")
	mountPathBase := flag.String("mount-path-base", "{cluster_name}", "Directory path as mount point")
	pageSize := flag.Int("page", 10, "The number of documents to list in one directory")
	// TODO: updateInterval := flag.Int("update-interval", 10, "Interval seconds of same queries to Elasticsearch")
	debugMode := flag.Bool("debug", false, "Emit debug logs")
	versionMode := flag.Bool("version", false, "Switch mode into version reporting")
	flag.Parse()

	// IF version arg is specified, report the app version and exit immediately.
	if *versionMode {
		log.Printf("%s %s\n", appName, appVersion)
		return
	}

	// Connect to Elasticsearch cluster and initialize the client
	dbURLsAsArray := strings.Split(*dbURLs, ",")
	db, err := elastic.NewClient(elastic.SetURL(dbURLsAsArray...))
	if err != nil {
		log.Fatalf("Failed to connect with db: url=%v\n", *dbURLs)
	}
	log.Println("Connected with db")

	// Fetch cluster name to decide the default mount path
	mountPath := strings.Replace(*mountPathPtr, "{mount-path-dir}", *mountPathDir, 1)
	mountPath = strings.Replace(mountPath, "{mount-path-base}", *mountPathBase, 1)
	if strings.Index(mountPath, "{cluster_name}") != -1 {
		resp, err := db.ClusterHealth().Do(context.Background())
		if err != nil {
			log.Fatalf("Failed to fetch cluster health: error=%v\n", err)
		}
		mountPath = strings.Replace(mountPath, "{cluster_name}", resp.ClusterName, -1)
	}

	// Ensure that mount path exists
	_, err = os.Stat(mountPath)
	madeMountPathBase := false
	if err != nil {
		if os.IsNotExist(err) {
			_, err = os.Stat(path.Dir(mountPath))
			if err != nil {
				if os.IsNotExist(err) {
					log.Fatalf("Need to make <mount-path-dir> directory by yourself")
				} else {
					log.Fatalf("Fatal to make directory for mount: error=%v\n", err)
				}
			}
			log.Println("Make the directory as mount path: %v\n", mountPath)
			madeMountPathBase = true
			err = os.Mkdir(mountPath, 0)
			if err != nil {
				log.Fatalf("Fatal to make directory for mount: error=%v\n", err)
			}
		} else {
			log.Fatalf("Fatal to stat mount directory: error=%v\n", err)
		}
	}

	// Create the filesystem is specialized for Elasticsearch
	if err != nil {
		log.Fatalf("Failed to new client: error=%v\n", err)
	}
	fs := elasticSearchFs{pathfs.NewDefaultFileSystem(), *debugMode, *pageSize,
		elasticSearchCache{elasticSearchClient{db}, *pageSize, nil, nil, nil, nil}}
	pathNodefs := pathfs.NewPathNodeFs(&fs, nil)

	// Start the FUSE server
	fuseServer, _, err := nodefs.MountRoot(mountPath, pathNodefs.Root(), nil)
	if err != nil {
		log.Fatalf("Failed to mount root: error=%v\n", err)
	}
	log.Println("Mounted")
	fuseServer.Serve()

	// Finish the application
	log.Println("Finished")
	if madeMountPathBase {
		log.Println("Remove directory as mount path")
		err = os.Remove(mountPath)
		if err != nil {
			log.Fatalf("Failed to remove directory as mount path")
		}
	}
}
