package main

import (
	"flag"
	"log"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	elastic "gopkg.in/olivere/elastic.v5"
)

type elasticSearchFs struct {
	pathfs.FileSystem
	db *elastic.Client
}

func (fs *elasticSearchFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" || name == "foo" || name == "bar" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0555}, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (fs *elasticSearchFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	if name == "" {
		names, err := fs.db.IndexNames()
		if err != nil {
			log.Fatalf("Failed to get index names: error=%v¥n", err)
			return nil, fuse.ENOENT
		}
		for _, name := range names {
			c = append(c, fuse.DirEntry{Name: name, Mode: fuse.S_IFDIR})
		}
		return c, fuse.OK
	}
	return nil, fuse.ENOENT
}

func main() {
	flag.Parse()
	if len(flag.Args()) < 2 {
		log.Fatal("Usage: elasticsearch-fuse ELASTICSEARCH_URL MOUNT_PATH")
	}
	dbURL := flag.Arg(0)
	mountPath := flag.Arg(1)

	dbClient, err := elastic.NewClient(elastic.SetURL(dbURL))
	if err != nil {
		log.Fatalf("Failed to new client: error=%v¥n", err)
	}
	fs := pathfs.NewPathNodeFs(&elasticSearchFs{pathfs.NewDefaultFileSystem(), dbClient}, nil)

	fuseServer, _, err := nodefs.MountRoot(mountPath, fs.Root(), nil)
	if err != nil {
		log.Fatalf("Failed to mount root: error=%v¥n", err)
	}
	fuseServer.Serve()
}
