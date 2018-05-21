package main

import (
	"log"
	"strconv"
	"strings"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

type ElasticsearchFS struct {
	pathfs.FileSystem

	cache *ElasticsearchCache
	debug bool
}

func NewElasticsearchFS(urls string, pageSize int, debug bool) (*ElasticsearchFS, error) {
	cache, err := NewElasticsearchCache(urls, pageSize)
	if err != nil {
		return nil, err
	}
	var fs ElasticsearchFS
	fs.FileSystem = pathfs.NewDefaultFileSystem()
	fs.cache = cache
	fs.debug = debug
	return &fs, nil
}

func (fs *ElasticsearchFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if fs.debug {
		log.Printf("GetAttr: name=%v\n", name)
	}

	// Return the attribute of the root directory
	if name == "" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
	}

	// Return the attribute of the index directory
	nameElems := strings.Split(name, "/")
	if len(nameElems) == 1 {
		indexs, err := fs.cache.EnsureIndexNames()
		if err != nil {
			log.Fatalf("Failed to ensure the index names: err=%v\n", err)
		}
		for _, index := range indexs {
			if nameElems[0] == index {
				return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
			}
		}
	}

	// Return the attributes of the document type directory
	if len(nameElems) == 2 {
		dtypes, err := fs.cache.EnsureDocumentTypes(nameElems[0])
		if err != nil {
			log.Fatalf("Failed to ensure the document types: index=%v, err=%v\n", nameElems[0], err)
		}
		for _, dtype := range dtypes {
			if nameElems[1] == dtype {
				return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
			}
		}
	}

	// Return the attributes of the paging directory
	if len(nameElems) == 3 {
		total, err := fs.cache.EnsureDocumentTotal(nameElems[0], nameElems[1])
		if err != nil {
			log.Fatalf("Failed to ensure the docs: index=%v, dtype=%v, err=%v\n", nameElems[0], nameElems[1], err)
		}
		page, err := strconv.Atoi(nameElems[2])
		if err != nil {
			log.Fatalf("Failed to parse the paging directory name as integer: index=%v, dtype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], nameElems[2], err)
		}
		from := int64(fs.cache.pageSize * page)
		if from < total {
			return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
		}
	}

	// Return the attributes of the document file
	if len(nameElems) == 4 {
		page, err := strconv.ParseInt(nameElems[2], 10, 0)
		if err != nil {
			log.Fatalf("Failed to parse the paging directory name as integer: index=%v, dtype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], nameElems[2], err)
		}
		docs, err := fs.cache.EnsureDocuments(nameElems[0], nameElems[1], int(page))
		if err != nil {
			log.Fatalf("Failed to ensure the docs: index=%v, dtype=%v, err=%v\n", nameElems[0], nameElems[1], err)
		}
		docSource, ok := docs[nameElems[3]]
		fileSize := uint64(len(docSource))
		if ok {
			return &fuse.Attr{Mode: fuse.S_IFREG | 0444, Size: fileSize}, fuse.OK
		}
	}
	return nil, fuse.ENOENT
}

func (fs *ElasticsearchFS) OpenDir(name string, context *fuse.Context) (entries []fuse.DirEntry, st fuse.Status) {
	if fs.debug {
		log.Printf("OpenDir: name=%v\n", name)
	}

	// If the root directory is opened, list up index names as the directory entries.
	if name == "" {
		indexs, err := fs.cache.EnsureIndexNames()
		if err != nil {
			log.Fatalf("Failed to ensure the index names: err=%v\n", err)
		}
		for _, index := range indexs {
			entries = append(entries, fuse.DirEntry{Name: index, Mode: fuse.S_IFDIR})
		}
		return entries, fuse.OK
	}

	// If the index directory is opened, list up document types as the directory entries.
	nameElems := strings.Split(name, "/")
	if len(nameElems) == 1 {
		dtypes, err := fs.cache.EnsureDocumentTypes(nameElems[0])
		if err != nil {
			log.Fatalf("Failed to ensure the document types: index=%v, err=%v\n", nameElems[0], err)
		}
		for _, dtype := range dtypes {
			entries = append(entries, fuse.DirEntry{Name: dtype, Mode: fuse.S_IFDIR})
		}
		return entries, fuse.OK
	}

	// If the document type directory is opened, list up docs as the file entries.
	if len(nameElems) == 2 {
		total, err := fs.cache.EnsureDocumentTotal(nameElems[0], nameElems[1])
		if err != nil {
			log.Fatalf("Failed to ensure the docs: index=%v, dtype=%v, err=%v\n", nameElems[0], nameElems[1], err)
		}
		for i := 0; int64(i*fs.cache.pageSize) < total; i++ {
			entries = append(entries, fuse.DirEntry{Name: strconv.Itoa(i), Mode: fuse.S_IFREG})
		}
		return entries, fuse.OK
	}

	// If the document type directory is opened, list up docs as the file entries.
	if len(nameElems) == 3 {
		page, err := strconv.Atoi(nameElems[2])
		if err != nil {
			log.Fatalf("Failed to parse the paging directory name as integer: index=%v, dtype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], nameElems[2], err)
		}
		docs, err := fs.cache.EnsureDocuments(nameElems[0], nameElems[1], int(page))
		if err != nil {
			log.Fatalf("Failed to ensure the docs: index=%v, dtype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], nameElems[2], err)
		}
		for docID := range docs {
			entries = append(entries, fuse.DirEntry{Name: docID, Mode: fuse.S_IFREG})
		}
		return entries, fuse.OK
	}

	return nil, fuse.ENOENT
}

func (fs *ElasticsearchFS) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, st fuse.Status) {
	if fs.debug {
		log.Printf("Open: name=%v, flags=%x\n", name, flags)
	}

	nameElems := strings.Split(name, "/")
	if len(nameElems) == 4 {
		page, err := strconv.Atoi(nameElems[2])
		if err != nil {
			log.Fatalf("Failed to parse the paging directory name as integer: index=%v, dtype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], nameElems[2], err)
		}
		docs, err := fs.cache.EnsureDocuments(nameElems[0], nameElems[1], page)
		if err != nil {
			log.Fatalf("Failed to ensure the docs: index=%v, dtype=%v, page=%v, err=%v\n", nameElems[0], nameElems[1], page, err)
		}
		docSource, ok := docs[nameElems[3]]
		if ok {
			return nodefs.NewDataFile(docSource), fuse.OK
		}
	}
	return nil, fuse.ENOENT
}
