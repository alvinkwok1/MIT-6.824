#!/bin/bash
rm -rf mr-*
go run -race mrcoordinator.go pg-*.txt
