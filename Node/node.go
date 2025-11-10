package main

import (
	p "consensus/protoc"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
)

type Server struct {
	p.UnimplementedMessageServiceServer
	clients    []int
	ServerPort int
}

func main() {
	s := &Server{}
	s.ServerPort = 5000
	//go MessageBroker(s)
	grpcServer := grpc.NewServer()
	var lis net.Listener
	var err error

	// Dynamically assign services to ports from 5001 to 5050
	for i := 5001; i < 5050; i++ {
		s.ServerPort++
		lis, err = net.Listen("tcp", fmt.Sprintf(":%d", s.ServerPort))
		if err != nil {
			log.Println("Port in use: ", s.ServerPort, ". Trying next port in line...")
			continue
		} else {
			break
		}
	}

	log.Printf("[SERVER]: Server has started, listening at %v", lis.Addr())
	p.RegisterMessageServiceServer(grpcServer, s)
	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	log.Println("[SERVER]: Server has stopped.")
}
