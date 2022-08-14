#!/bin/bash
go build -race -ldflags=-w -buildmode=plugin ../mrapps/wc.go
go build -race mrworker.go wc.so -o mrworker
gdb mrworker wc.so