package main

import (
	"bufio"
	p "consensus/protoc"
	"fmt"
	"os"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"log"
	"net"
)

type Server struct {
	p.UnimplementedMessageServiceServer
	clients    []int
	ServerPort int
}

/*type Message struct {
	isReply bool
	Timestamp int64
	Process_id int
}*/

func (s *Server) StartDiscovery() {
	log.Println("Starting discovery service")
	for i := 5001; i < 5050; i++ {
		if i == s.ServerPort {
			continue
		}

		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(1000))
		resp, err := s.SendMessage(ctx, &p.Message{IsReply: false, Timestamp: 0, ProcessId: 0}, i)
		if err != nil {
			// Port not in use or is dead
			log.Printf("Port not in use: %d", i)
		}
		if resp != nil {
			s.clients = append(s.clients, i)
			log.Printf("Port registered: %d", i)
		}
	}
}

func (s *Server) SendMessage(ctx context.Context, in *p.Message, receivingPort int) (*p.Message, error) {
	conn, err := grpc.NewClient(fmt.Sprintf(":%d", receivingPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	client := p.NewMessageServiceClient(conn)
	//client.MessageService(ctx, in)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return client.MessageService(ctx, in)
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

	if s.ServerPort == 5001 {
		log.Println(" --- READY TO START NETWORK, PRESS ANY BUTTON TO CONTINUE ---")
		log.Println(" --- THIS WILL START WITH ALL CURRENTLY READY NODES ---")
		bufio.NewReader(os.Stdin).ReadBytes('\n')
		go s.StartDiscovery()
	} else {
		log.Println(" --- WAITING FOR PORT 5001 TO START THE NETWORK ---")
	}

	log.Printf("Server has started, listening at %v", lis.Addr())
	p.RegisterMessageServiceServer(grpcServer, s)
	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	log.Println("Server has stopped.")
}
