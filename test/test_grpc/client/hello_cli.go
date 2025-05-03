package main

import (
	context "context"
	"errors"

	"github.com/yeyeye2333/PacificaMQ/test/test_grpc"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	conn, err := grpc.NewClient("localhost:50001", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		println("connect err", err.Error())
	}

	defer conn.Close()
	client := test_grpc.NewHelloClient(conn)
	res, err := client.TestRpc(context.Background(), &test_grpc.HelloRequest{Name: "yy"})
	status, _ := status.FromError(err)
	if status.Message() != errors.New("testErr").Error() {
		println("client not match,err:", status.Message(), errors.New("testErr").Error())
	} else {
		println("client match")
	}
	if res == nil {
		println("res is nil")
	}
	println(res.Message)
}
