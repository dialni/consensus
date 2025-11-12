package main

import (
	"bufio"
	p "consensus/protoc"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"log"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ServerState int

const (
	RELEASED ServerState = iota
	WANTED
	HELD
)

type LogicalTime struct {
	mu        sync.Mutex
	timestamp int64
}

type Server struct {
	p.UnimplementedMessageServiceServer
	NetworkActive bool
	clients       []int32
	ServerPort    int32
	lTime         LogicalTime
	RequestQueue  chan *p.Message
	ReplyQueue    chan p.Message
	DeferredQueue []p.Message
	ServerState   ServerState
}

// Services
func (s *Server) MessageService(ctx context.Context, in *p.Message) (*p.Message, error) {
	//log.Printf("Received MessageService: %v", in)
	if s.NetworkActive == false {
		return &p.Message{IsReply: true, Timestamp: s.lTime.timestamp, ProcessId: s.ServerPort}, nil
	}

	if in.IsReply {
		s.ReplyQueue <- *in
	} else {
		s.RequestQueue <- in
	}
	return &p.Message{IsReply: true, Timestamp: s.lTime.timestamp, ProcessId: s.ServerPort}, nil
}

/*
	func (s *Server) StartDiscoveryService(ctx context.Context, in *p.Message) (*p.Message, error) {
		//log.Printf("Received StartDiscoveryService: %v", in)
		s.StartDiscovery()
		return &p.Message{IsReply: true, Timestamp: 1, ProcessId: s.ServerPort}, nil
	}
*/
func (s *Server) StartNetworkService(ctx context.Context, in *p.Message) (*p.Message, error) {
	//log.Printf("Received StartNetworkService: %v", in)
	s.NetworkActive = true
	return &p.Message{IsReply: true, Timestamp: 1, ProcessId: s.ServerPort}, nil
}

// Discovery
/*
// Dynamic nth node discovery works well until under any kind of load, sadly.
func (s *Server) StartDiscovery() {
	time.Sleep(time.Duration(2000*(int(s.ServerPort)-5000)) * time.Millisecond)
	log.Println("Starting discovery service")
	var wg sync.WaitGroup
	for i := 1; i < 5; i++ {
		if i+5000 == int(s.ServerPort) {
			//log.Println("skipping ", i+5000)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := grpc.NewClient(fmt.Sprintf(":%d", i+5000), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("could not connect: %v\n", err)
			}
			defer conn.Close()

			client := p.NewMessageServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(3))
			defer cancel()
			resp, err := client.MessageService(ctx, &p.Message{IsReply: false, Timestamp: 1, ProcessId: s.ServerPort})
			if err != nil || resp == nil {
				return
			} else if resp.IsReply {
				//log.Println("Port registered", resp.ProcessId)
				s.clients = append(s.clients, resp.ProcessId)
			}
			return
		}()
	}
	wg.Wait()
	log.Println("Ports registered", s.clients)
	log.Println("discovery service finished")
}*/

func (s *Server) BroadcastDiscoveryMessage() {
	log.Println("Broadcasting discovery message")
	var wg sync.WaitGroup
	for _, client := range s.clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := grpc.NewClient(fmt.Sprintf(":%d", client), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("could not connect: %v\n", err)
			}
			defer conn.Close()

			c := p.NewMessageServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(5))
			defer cancel()
			resp, err := c.StartDiscoveryService(ctx, &p.Message{IsReply: false, Timestamp: 1, ProcessId: s.ServerPort})
			if err != nil || resp == nil {
				return
			}
			//log.Println("Sent 'StartDiscovery()' message to", resp.ProcessId)
			return
		}()
	}
	wg.Wait()
}

// Confirming everyone is active and starting the network
func (s *Server) BroadcastStartNetworkMessage() {
	log.Println("Broadcasting StartNetwork message")
	var wg sync.WaitGroup
	for _, client := range s.clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := grpc.NewClient(fmt.Sprintf(":%d", client), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("could not connect: %v\n", err)
			}
			defer conn.Close()

			c := p.NewMessageServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			resp, err := c.StartNetworkService(ctx, &p.Message{IsReply: false, Timestamp: 1, ProcessId: s.ServerPort})
			if err != nil || resp == nil {
				return
			}
			//log.Println("Sent 'StartNetwork()' message to", resp.ProcessId)
			return
		}()
	}
	wg.Wait()
	log.Println("Network is now active! Ready to compete for Critical Zone.")
}

func main() {
	// initialize server declare variables
	s := &Server{clients: make([]int32, 0), ServerPort: 5000,
		ServerState: RELEASED, NetworkActive: false,
		ReplyQueue: make(chan p.Message, 0), RequestQueue: make(chan *p.Message, 0),
		DeferredQueue: make([]p.Message, 0)}
	s.lTime.mu.Lock()
	s.lTime.timestamp = 1
	s.lTime.mu.Unlock()
	grpcServer := grpc.NewServer()
	var lis net.Listener
	var err error

	// Dynamically assign services to ports from 5001 to 5003 (3 active nodes, + critical zone on 5000)
	for i := 5001; i < 5004; i++ {
		s.ServerPort++
		lis, err = net.Listen("tcp", fmt.Sprintf(":%d", s.ServerPort))
		if err != nil {
			log.Println("Port ", s.ServerPort, " in use, trying next port in line...")
			continue
		} else {
			break
		}
	}
	for i := 5001; i < 5004; i++ {
		if int32(i) != s.ServerPort {
			s.clients = append(s.clients, int32(i))
		}
	}
	log.Println("My list of contacts:", s.clients)

	// only 5001 is allowed to start the algorithm, others will wait until receiving the first message in StartDiscovery
	if s.ServerPort == 5001 {
		log.Println(" --- READY TO START NETWORK, PRESS ANY BUTTON TO CONTINUE ---")
		log.Println(" --- THIS WILL START WITH ALL CURRENTLY READY NODES ---")
		bufio.NewReader(os.Stdin).ReadBytes('\n') // wait until user presses enter
		log.Println("Beginning process, please wait...")
		s.NetworkActive = true
		//s.StartDiscovery()
		s.BroadcastStartNetworkMessage()
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

// Handle incoming requests
func (s *Server) InsertIntoDeferredQueue(v p.Message) {
	for i := 0; i < len(s.DeferredQueue); i++ {
		if s.DeferredQueue[i].Timestamp == v.Timestamp || s.DeferredQueue[i].Timestamp > v.Timestamp {
			if s.DeferredQueue[i].Timestamp == v.Timestamp && s.DeferredQueue[i].ProcessId < v.ProcessId {
				continue
			}
			s.DeferredQueue = append(s.DeferredQueue[:i+1], s.DeferredQueue[i:]...)
			s.DeferredQueue[i] = v
			return
		}
	}
	s.DeferredQueue = append(s.DeferredQueue, v)
	return
}

func (s *Server) ReplyToRequest(msg p.Message) {
	conn, err := grpc.NewClient(fmt.Sprintf(":%d", msg.ProcessId), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("could not connect: %v\n", err)
	}
	defer conn.Close()

	c := p.NewMessageServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(999))
	defer cancel()
	resp, err := c.MessageService(ctx, &p.Message{IsReply: true, Timestamp: 1, ProcessId: s.ServerPort})
	if err != nil || resp == nil {
		return
	}
	//log.Println("Sent 'StartNetwork()' message to", resp.ProcessId)
	return
}

func (s *Server) CheckDeferredQueue() {
	// if queue is empty, first message is higher timestamp than servers or we are accessing CZ, return
	var msg p.Message
	for {
		if len(s.DeferredQueue) == 0 || s.DeferredQueue[0].Timestamp > s.lTime.timestamp || s.ServerState == HELD {
			return
		}
		if s.ServerState == RELEASED {
			msg = s.DeferredQueue[0]
			s.DeferredQueue = s.DeferredQueue[1:]
			s.lTime.mu.Lock()
			if s.lTime.timestamp < msg.Timestamp {
				s.lTime.timestamp = msg.Timestamp
			}
			s.lTime.timestamp++
			s.lTime.mu.Unlock()
			s.ReplyToRequest(msg)
		} else if s.ServerState == WANTED {
			msg = s.DeferredQueue[0]
			if msg.Timestamp > s.lTime.timestamp {
				return
			} else if msg.Timestamp == s.lTime.timestamp && s.ServerPort < msg.ProcessId {
				return
			} else {
				s.lTime.mu.Lock()
				if s.lTime.timestamp < msg.Timestamp {
					s.lTime.timestamp = msg.Timestamp
				}
				s.lTime.timestamp++
				s.lTime.mu.Unlock()
				s.DeferredQueue = s.DeferredQueue[1:]
				s.ReplyToRequest(msg)
			}
		}
	}
}

func (s *Server) ListenForRequests() {
	log.Println("Listening for requests...")
	var msg *p.Message
	for {
		s.CheckDeferredQueue()
		msg = <-s.RequestQueue
		switch s.ServerState {
		case RELEASED:
			s.lTime.mu.Lock()
			if s.lTime.timestamp < msg.Timestamp {
				s.lTime.timestamp = msg.Timestamp
			}
			s.lTime.timestamp++
			s.lTime.mu.Unlock()
			s.ReplyToRequest(*msg)
		case WANTED:
			if s.lTime.timestamp < msg.Timestamp {
				s.InsertIntoDeferredQueue(*msg)
			} else if s.lTime.timestamp == msg.Timestamp {
				if s.ServerPort < msg.ProcessId {
					s.InsertIntoDeferredQueue(*msg)
				} else {
					s.lTime.mu.Lock()
					if s.lTime.timestamp < msg.Timestamp {
						s.lTime.timestamp = msg.Timestamp
					}
					s.lTime.timestamp++
					s.lTime.mu.Unlock()
					s.ReplyToRequest(*msg)
				}
			} else {
				s.lTime.mu.Lock()
				if s.lTime.timestamp < msg.Timestamp {
					s.lTime.timestamp = msg.Timestamp
				}
				s.lTime.timestamp++
				s.lTime.mu.Unlock()
				s.ReplyToRequest(*msg)
			}
		case HELD:
			s.InsertIntoDeferredQueue(*msg)
		}
	}
}

// StartNetwork will wait for the network to be active and then start all requests
func (s *Server) StartNetwork() {
	for {
		// Keep this loop busy until our server is ready to start
		if s.NetworkActive == true {
			break
		}
	}
	log.Println("Ready to start algorithm")
	go s.ListenForRequests()
	time.Sleep(time.Duration(1500) * time.Millisecond)
	go s.RequestAccessToCriticalZone()

}

func (s *Server) RequestAccessToCriticalZone() {
	log.Println("I will now request access from time to time.")
	var wg sync.WaitGroup
	for {
		time.Sleep(time.Duration(1000) * time.Millisecond)
		if rand.Intn(3) == 0 {
			s.ServerState = WANTED
			for _, client := range s.clients {
				log.Println("Sending request to client", client)
				wg.Add(1)
				go func() {
					defer wg.Done()
					conn, err := grpc.NewClient(fmt.Sprintf(":%d", client), grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						log.Printf("could not connect: %v\n", err)
					}
					defer conn.Close()

					c := p.NewMessageServiceClient(conn)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(5))
					defer cancel()
					resp, err := c.MessageService(ctx, &p.Message{IsReply: false, Timestamp: 1, ProcessId: s.ServerPort})
					if err != nil {
						log.Printf("error sending message: %v\n", err)
						return
					} else if resp == nil {
						log.Printf("error sending message: %v\n", resp)
					}
					return
				}()
			}
			wg.Wait()
			for range s.clients {
				<-s.ReplyQueue
				log.Println("Received reply")
			}
			s.EnterCriticalZone()
		}
	}
}

func (s *Server) EnterCriticalZone() {
	s.ServerState = HELD
	s.ReplyToRequest(p.Message{IsReply: true, Timestamp: s.lTime.timestamp, ProcessId: 5000})
	s.ServerState = RELEASED
}
