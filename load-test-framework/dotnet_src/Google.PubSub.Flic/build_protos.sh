#!/bin/sh

protoc.exe -I../../proto --csharp_out=.  ../../proto/loadtest.proto
