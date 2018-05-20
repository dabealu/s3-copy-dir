#!/bin/bash
set -e
SRC_DIR=/usr/src/s3-copy-dir
docker run --rm \
    -v $PWD:$SRC_DIR \
    -w $SRC_DIR golang:1.10-stretch \
    bash -c "go get github.com/minio/minio-go && go build -v"
