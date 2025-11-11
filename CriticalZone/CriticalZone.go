package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	p "consensus/protoc"

	"google.golang.org/grpc"
)

func main() {
	cz := &CriticalZone{inUse: false /* ... */}
	cz.Listen(6000)
}

type CriticalZone struct {
	p.UnimplementedMessageServiceServer
	inUse bool
}

func (cz *CriticalZone) MessageService(ctx context.Context, msg *p.Message) (*p.Message, error) {
	if !cz.inUse {
		cz.inUse = true
		log.Println("Process ", msg.ProcessId, " entered the critical section.")

		workTime := time.Duration(1000) * time.Millisecond
		log.Println("Process ", msg.ProcessId, " is working for ", workTime)
		time.Sleep(workTime)

		cz.inUse = false
		log.Println("Process ", msg.ProcessId, " left the critical section.")
	} else {
		log.Println("Critical section is busy. Process ", msg.ProcessId, " must wait.")
	}

	reply := &p.Message{
		IsReply:   true,
		Timestamp: msg.Timestamp,
		ProcessId: msg.ProcessId,
	}
	return reply, nil
}

func (cz *CriticalZone) Listen(port int) {
	address := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Println("Could not start CriticalZone on port", port)
		return
	}

	server := grpc.NewServer()
	p.RegisterMessageServiceServer(server, cz)

	fmt.Println("CriticalZone running on port", port)
	if err := server.Serve(lis); err != nil {
		log.Println("Server error:", err)
	}
}
