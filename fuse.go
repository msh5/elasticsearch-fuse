package main

import (
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func MountFilesystem(fs pathfs.FileSystem, point string) error {
	nodeFs := pathfs.NewPathNodeFs(fs, nil)
	server, _, err := nodefs.MountRoot(point, nodeFs.Root(), nil)
	if err != nil {
		return err
	}
	server.Serve()
	return nil
}
