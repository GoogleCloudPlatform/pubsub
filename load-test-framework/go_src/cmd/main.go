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
