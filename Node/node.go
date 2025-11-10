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

type ServerState int

const (
	RELEASED ServerState = iota
	WANTED
	HELD
)

type Server struct {
	p.UnimplementedMessageServiceServer
	clients      []int
	ServerPort   int32
	lTime        int64
	MessageQueue chan p.Message
	PriorityList []p.Message
	ServerState  ServerState
}

/*type Message struct {
	isReply bool
	Timestamp int64
	Process_id int
}*/

// StartDiscovery scans for other nodes on different ports from a pre-defined range and adds them to s.clients.
func (s *Server) StartDiscovery() {
	log.Println("Starting discovery service")
	for i := 5001; i < 5010; i++ {
		// skip our own port
		if i == int(s.ServerPort) {
			continue
		}
		// send message, other potential nodes have 1 second to respond. otherwise they are disregarded.
		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(1000))
		resp, err := s.SendMessage(ctx, &p.Message{IsReply: false, Timestamp: 0, ProcessId: 0}, i)
		if err != nil || resp == nil {
			// Port not in use or is dead
			log.Printf("Port not in use: %d", i)
			continue
		} else if resp.IsReply == true {
			s.clients = append(s.clients, i)
			ctx.Done()
			log.Printf("Port registered: %d", i)
		}
	}
	log.Println("Discovery service finished, ports: ", s.clients)
}

// SendMessage starts a new client and sends our message to the selected port. returns Message.
func (s *Server) SendMessage(ctx context.Context, in *p.Message, receivingPort int) (*p.Message, error) {
	conn, err := grpc.NewClient(fmt.Sprintf(":%d", receivingPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	client := p.NewMessageServiceClient(conn)
	//resp, _ := client.MessageService(ctx, in)

	return client.MessageService(ctx, in)
}

// MessageService is the function that is run when a node receives a message from another node.
func (s *Server) MessageService(ctx context.Context, in *p.Message) (*p.Message, error) {
	log.Printf("Received message: %v", in.ProcessId)
	return &p.Message{IsReply: true, Timestamp: 1, ProcessId: s.ServerPort}, nil
}

// this should probably be using mutexs or some other smart way of establishing the insertion sort
func (s *Server) ListenForMessages() {
	s.MessageQueue = make(chan p.Message, 0)
	var incomingMessage p.Message
	for {
		incomingMessage = <-s.MessageQueue
		//s.PriorityList = EnQueue(s.PriorityList, incomingMessage)
	}
}

func main() {
	// initialize server declare variables
	s := &Server{lTime: 1, clients: make([]int, 0), ServerPort: 5000, ServerState: RELEASED}
	grpcServer := grpc.NewServer()
	var lis net.Listener
	var err error

	// Dynamically assign services to ports from 5001 to 5050
	for i := 5001; i < 5010; i++ {
		s.ServerPort++
		lis, err = net.Listen("tcp", fmt.Sprintf(":%d", s.ServerPort))
		if err != nil {
			log.Println("Port in use: ", s.ServerPort, ". Trying next port in line...")
			continue
		} else {
			break
		}
	}

	// only 5001 is allowed to start the algorithm, others will wait until receiving the first message in StartDiscovery
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
