package test_grpc

// import (
// 	context "context"
// 	"fmt"
// 	"log"
// 	"net"

// 	grpc "google.golang.org/grpc"
// )

// type hello struct {
// 	UnimplementedHelloServer
// }

// func (h *hello) TestRpc(ctx context.Context, in *HelloRequest) (*HelloResponse, error) {
// 	out := "hello" + in.GetName()
// 	return &HelloResponse{Message: out}, nil
// }

// func main() {
// 	port := 50001
// 	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
// 	if err != nil {
// 		log.Fatalf("failed to listen: %v", err)
// 	}
// 	grpcServer := grpc.NewServer()
// 	RegisterHelloServer(grpcServer, &hello{})
// 	grpcServer.Serve(lis)
// 	grpcServer.Stop()
// }
