#!/bin/bash
# only for development

mkdir -p tmp
go run main.go http://localhost:9200 tmp
