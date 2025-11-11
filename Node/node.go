package main

import (
	"bufio"
	p "consensus/protoc"
	"fmt"
	"math/rand"
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
	NetworkActive bool
	clients       []int
	ServerPort    int32
	lTime         int64
	RequestQueue  chan p.Message
	ReplyQueue    chan p.Message
	PriorityList  []p.Message
	ServerState   ServerState
}

/*type Message struct {
	isReply bool
	Timestamp int64
	Process_id int
}*/

// StartDiscovery scans for other nodes on different ports from a pre-defined range and adds them to s.clients.
func (s *Server) StartDiscovery() {
	log.Println("Starting discovery service")
	time.Sleep(time.Duration(500*(int(s.ServerPort)-5000)) * time.Millisecond)
	for i := 5001; i < 5010; i++ {
		// skip our own port
		if i == int(s.ServerPort) {
			continue
		}
		// send message, other potential nodes have 1 second to respond. otherwise they are disregarded.
		resp, err := s.SendStartNetworkMessage(int32(i))
		if err != nil || resp == nil {
			// Port not in use or is dead
			//log.Printf("Port not in use: %d", i)
			continue
		} else if resp.IsReply == true {
			s.clients = append(s.clients, i)
			log.Printf("Port registered: %d", i)
		}
	}
	log.Println("Discovery service finished, ports: ", s.clients)
}

// SendMessage starts a new client and sends our message to the selected port. returns Message.
func (s *Server) SendMessage(ctx context.Context, in *p.Message, receivingPort int32) (*p.Message, error) {
	conn, err := grpc.NewClient(fmt.Sprintf(":%d", receivingPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	client := p.NewMessageServiceClient(conn)
	//resp, _ := client.MessageService(ctx, in)

	return client.MessageService(ctx, in)
}
func (s *Server) SendStartNetworkMessage(receivingPort int32) (*p.Message, error) {
	conn, err := grpc.NewClient(fmt.Sprintf(":%d", receivingPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()
	client := p.NewMessageServiceClient(conn)
	//resp, _ := client.MessageService(ctx, in)
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(3000))
	defer ctx.Done()
	resp, err := client.StartNetworkService(ctx, &p.Message{IsReply: true, Timestamp: 1, ProcessId: s.ServerPort})
	//log.Println(resp)
	return resp, err
}

// MessageService is the function that is run when a node receives a message from another node.
func (s *Server) MessageService(ctx context.Context, in *p.Message) (*p.Message, error) {
	log.Printf("Received message: %v", in.ProcessId)
	if in.GetIsReply() {
		return &p.Message{IsReply: true, Timestamp: 1, ProcessId: s.ServerPort}, nil
	} else {
		return &p.Message{IsReply: false, Timestamp: 1, ProcessId: s.ServerPort}, nil
	}
}

func (s *Server) StartNetworkService(ctx context.Context, in *p.Message) (*p.Message, error) {
	//log.Printf("Start network message: %v", in.ProcessId)
	if !s.NetworkActive {
		s.NetworkActive = true
	}
	return &p.Message{IsReply: true, Timestamp: 1, ProcessId: s.ServerPort}, nil
}

// this should probably be using mutexs or some other smart way of establishing the insertion sort
func (s *Server) ListenForRequests() {
	s.RequestQueue = make(chan p.Message, 0)
	var incomingMessage p.Message
	for {
		incomingMessage = <-s.RequestQueue
		if s.ServerState == HELD {
			// Defer message into sorted list
		} else if s.ServerState == WANTED {
			// If Message came before, give priority to Message sender
			if incomingMessage.Timestamp > s.lTime {
				ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(9999999))
				_, _ = s.SendMessage(ctx, &p.Message{IsReply: true, Timestamp: s.lTime, ProcessId: s.ServerPort}, incomingMessage.ProcessId)
			} else if incomingMessage.Timestamp == s.lTime {
				if s.ServerPort < incomingMessage.ProcessId {
					ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(9999999))
					_, _ = s.SendMessage(ctx, &p.Message{IsReply: true, Timestamp: s.lTime, ProcessId: s.ServerPort}, incomingMessage.ProcessId)
				} else {
					// Defer message into sorted list
				}
			}
		} else {
			ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(9999999))
			_, _ = s.SendMessage(ctx, &p.Message{IsReply: true, Timestamp: s.lTime, ProcessId: s.ServerPort}, incomingMessage.ProcessId)
		}
	}
}

func (s *Server) EnterCriticalSection() {

	ctx := context.Background()
	s.SendMessage(ctx, &p.Message{IsReply: true, Timestamp: 1, ProcessId: s.ServerPort}, 5000)

	s.ServerState = RELEASED
}

func main() {
	// initialize server declare variables
	s := &Server{lTime: 1, clients: make([]int, 0), ServerPort: 5000, ServerState: RELEASED, NetworkActive: false}
	grpcServer := grpc.NewServer()
	var lis net.Listener
	var err error

	// Dynamically assign services to ports from 5001 to 5050
	for i := 5001; i < 5010; i++ {
		s.ServerPort++
		lis, err = net.Listen("tcp", fmt.Sprintf(":%d", s.ServerPort))
		if err != nil {
			log.Println("Port ", s.ServerPort, " in use, trying next port in line...")
			continue
		} else {
			break
		}
	}

	// only 5001 is allowed to start the algorithm, others will wait until receiving the first message in StartDiscovery
	if s.ServerPort == 5001 {
		log.Println(" --- READY TO START NETWORK, PRESS ANY BUTTON TO CONTINUE ---")
		log.Println(" --- THIS WILL START WITH ALL CURRENTLY READY NODES ---")
		bufio.NewReader(os.Stdin).ReadBytes('\n') // wait until user presses enter
		s.NetworkActive = true
		go s.StartNetwork()
	} else {
		log.Println(" --- WAITING FOR PORT 5001 TO START THE NETWORK ---")
		go s.StartNetwork()
	}

	log.Printf("Server has started, listening at %v", lis.Addr())
	p.RegisterMessageServiceServer(grpcServer, s)

	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// StartNetwork will wait for the network to be active and then start all requests
func (s *Server) StartNetwork() {
	for {
		// Keep this loop busy until our server is ready to start
		if s.NetworkActive == true {
			break
		}
		time.Sleep(time.Duration(500) * time.Millisecond)
	}
	s.StartDiscovery()
	log.Println("Ready to start algorithm")
	//go s.ListenForRequests()
	//go s.Ask()
}

func (s *Server) Ask() {
	for {
		//Ask at random intervals as long as node is not in critical section or waiting to go into it
		delay := time.Duration(rand.Intn(1000)) * time.Millisecond
		time.Sleep(delay)

		if s.ServerState == RELEASED {
			//Ask all ports if critical section is free
			s.ServerState = WANTED
			for _, client := range s.clients {
				ctx, _ := context.WithTimeout(context.Background(), time.Second*time.Duration(999999))
				resp, _ := s.SendMessage(ctx, &p.Message{IsReply: false, Timestamp: s.lTime, ProcessId: s.ServerPort}, int32(client))
				s.ReplyQueue <- *resp
			}
			s.ServerState = HELD
		}
	}
}
