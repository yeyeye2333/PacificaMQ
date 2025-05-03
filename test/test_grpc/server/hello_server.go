package main

import (
	context "context"
	"errors"
	"fmt"
	"log"
	"net"

	"github.com/yeyeye2333/PacificaMQ/test/test_grpc"
	grpc "google.golang.org/grpc"
)

type hello struct {
	test_grpc.UnimplementedHelloServer
}

func (h *hello) TestRpc(ctx context.Context, in *test_grpc.HelloRequest) (*test_grpc.HelloResponse, error) {
	var err = errors.New("testErr")
	if err.Error() == errors.New("testErr").Error() {
		println("server error matched, but")
	} else {
		println("server error not matched")
	}
	return &test_grpc.HelloResponse{}, err
	// println("Received: " + in.GetName())
	// out := "hello" + in.GetName()
	// return &test_grpc.HelloResponse{Message: out}, nil
}

func main() {
	port := 50001
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	test_grpc.RegisterHelloServer(grpcServer, &hello{})
	grpcServer.Serve(lis)
	grpcServer.Stop()
}
