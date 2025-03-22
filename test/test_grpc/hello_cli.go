package test_grpc

import (
	context "context"

	grpc "google.golang.org/grpc"
)

func main() {
	conn, _ := grpc.Dial("localhost:50001")

	defer conn.Close()
	client := NewHelloClient(conn)
	client.TestRpc(context.Background(), &HelloRequest{Name: "yy"})
}
