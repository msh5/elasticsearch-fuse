#!/bin/bash
# only for development

mkdir -p tmp
go run main.go --servers http://localhost:9200 --mount-path tmp --debug
