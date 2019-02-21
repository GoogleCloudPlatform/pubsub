/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package main

import (
	"google.com/cloud_pubsub_loadtest/internal/genproto"
	"flag"
	"fmt"
	"google.com/cloud_pubsub_loadtest/internal"
	"google.golang.org/grpc"
	"log"
	"net"
)

var port = flag.Int("port", 5000, "The port to use for this server")
var publisher = flag.Bool("publisher", true, "Run the publisher if true or subscriber if false")

func main() {
	flag.Parse()
	var workerType string
	if *publisher {
		workerType = "publisher"
	} else {
		workerType = "subscriber"
	}
	log.Printf("Starting %s at port: %d", workerType, *port)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	genproto.RegisterLoadtestWorkerServer(grpcServer, internal.NewLoadtestWorkerService(*publisher))
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
