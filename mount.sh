#!/bin/bash
# only for development

mkdir -p tmp
go run main.go -db http://localhost:9200 -mp tmp -debug
