package broker

import (
	"context"
	"io"
	"log"
	"net"

	"github.com/yeyeye2333/PacificaMQ/api/broker_rpc"
	"github.com/yeyeye2333/PacificaMQ/internal/registry"
	regCM "github.com/yeyeye2333/PacificaMQ/internal/registry/common"
	"google.golang.org/grpc"
)

type Server struct {
	broker_rpc.UnimplementedBrokerServer
	ctx             context.Context
	addr            string
	grpcServer      *grpc.Server
	partitionOpts   *partitionOptions
	registryOpts    *regCM.Options
	registry        regCM.Registry
	topic2partition map[string]map[int32]*partition
}

func (s *Server) newPartitionOptions(partID int32) *partitionOptions {
	return &partitionOptions{
		StorageOpts:  s.partitionOpts.StorageOpts,
		PacificaOpts: s.partitionOpts.PacificaOpts,
		RegistryOpts: s.partitionOpts.RegistryOpts,
		PartitionID:  partID,
		MaxTimeOut:   s.partitionOpts.MaxTimeOut,
	}
}

func (s *Server) Process(event *regCM.Event) {
	if event.Type == regCM.PutBroker {
		brokerInfo := event.Data[0].(*regCM.BrokerInfo)
		if brokerInfo.Address == s.addr {
			for i := 0; i < len(brokerInfo.NewPartitions.GetTopicName()); i++ {
				var part *partition
				if pMap, ok := s.topic2partition[brokerInfo.NewPartitions.GetTopicName()[i]]; !ok {
					part, _ = NewPartition(s.ctx, s.newPartitionOptions(brokerInfo.NewPartitions.GetPartitionNum()[i]))
					s.topic2partition[brokerInfo.NewPartitions.GetTopicName()[i]] = make(map[int32]*partition)
					s.topic2partition[brokerInfo.NewPartitions.GetTopicName()[i]][brokerInfo.NewPartitions.GetPartitionNum()[i]] = part
				} else {
					if _, ok := pMap[brokerInfo.NewPartitions.GetPartitionNum()[i]]; !ok {
						part, _ = NewPartition(s.ctx, s.newPartitionOptions(brokerInfo.NewPartitions.GetPartitionNum()[i]))
						pMap[brokerInfo.NewPartitions.GetPartitionNum()[i]] = part
					}
				}
			}
		}
	}
}

func (s *Server) Start(ctx context.Context, opts *Options) error {
	s.registry, _ = registry.NewRegistry(opts.RegistryOpts)

	s.topic2partition = make(map[string]map[int32]*partition)

	s.addr = opts.Address
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s.grpcServer = grpc.NewServer()
	broker_rpc.RegisterBrokerServer(s.grpcServer, s)
	go s.grpcServer.Serve(lis)

	s.registry.SubBroker(s.addr, s)

	<-ctx.Done()
	s.grpcServer.Stop()

	return nil
}

// enum RetCode {
//     SUCCESS = 0;
//     NotLeader = 1;
//     TimeOut = 2;
//     ErrOther = 3;
// }
// message PushMessagesRequest {
//     message Topic{
//         message Partition{
//             optional int32 PartitionID = 1;
//             repeated bytes Datas = 2;
//             optional uint64 ProducerID = 3;
//             optional uint64 SequenceNumber = 4;
//         }
//         optional string TopicName = 1;
//         repeated Partition Partitions = 2;
//     }
//     repeated Topic Topics = 1;
// }

// message PushMessagesResponse {
//     message TopicResponses{
//         message PartitionResponses{
//             optional int32 PartitionID = 1;
//             optional RetCode Ret=2;
//             optional uint64 StartIndex = 3;
//         }
//         optional string TopicName = 1;
//         repeated PartitionResponses Partitions = 2;
//     }
//     repeated TopicResponses Topics = 1;
// }

// rpcServer 实现
func (s *Server) PushMessages(stream broker_rpc.Broker_PushMessagesServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		response := &broker_rpc.PushMessagesResponse{}
		for i, Topic := range in.GetTopics() {
			response.Topics = append(response.GetTopics(), &broker_rpc.PushMessagesResponse_TopicResponses{})
			if pMap, ok := s.topic2partition[Topic.GetTopicName()]; !ok {
				for ii := 0; ii < len(Topic.GetPartitions()); ii++ {
					response.GetTopics()[i].Partitions = append(response.GetTopics()[i].Partitions, &broker_rpc.PushMessagesResponse_TopicResponses_PartitionResponses{
						PartitionID: Topic.GetPartitions()[ii].PartitionID,
						Ret:         broker_rpc.RetCode_NotLeader.Enum(),
					})
				}
			} else {
				for ii := 0; ii < len(Topic.GetPartitions()); ii++ {
					part, ok := pMap[Topic.GetPartitions()[ii].GetPartitionID()]
					if !ok {
						response.GetTopics()[i].Partitions = append(response.GetTopics()[i].Partitions, &broker_rpc.PushMessagesResponse_TopicResponses_PartitionResponses{
							PartitionID: Topic.GetPartitions()[ii].PartitionID,
							Ret:         broker_rpc.RetCode_NotLeader.Enum(),
						})
					} else {
						partRes := part.pushMessage(Topic.GetPartitions()[ii])
						response.GetTopics()[i].Partitions = append(response.GetTopics()[i].Partitions, partRes)
					}
				}
			}
		}

		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

// message PullMessagesRequest {
//     message Topic{
//         message Partition{
//             optional int32 PartitionID = 1;
//             optional uint64 FetchIndex = 2;
//             optional uint32 MaxBytes = 3;
//         }
//         optional string TopicName = 1;
//         repeated Partition Partitions = 2;
//     }
//     repeated Topic Topics = 1;

// }

//	message PullMessagesResponse {
//	    message TopicResponses{
//	        message PartitionResponses{
//	            optional int32 PartitionID = 1;
//	            optional RetCode Ret=2;
//	            repeated bytes Datas = 3;
//	        }
//	        optional string TopicName = 1;
//	        repeated PartitionResponses Partitions = 2;
//	    }
//	    repeated TopicResponses Topics = 1;
//	}
func (s *Server) PullMessages(ctx context.Context, in *broker_rpc.PullMessagesRequest) (*broker_rpc.PullMessagesResponse, error) {
	response := &broker_rpc.PullMessagesResponse{}
	for i, Topic := range in.GetTopics() {
		response.Topics = append(response.GetTopics(), &broker_rpc.PullMessagesResponse_TopicResponses{})
		if pMap, ok := s.topic2partition[Topic.GetTopicName()]; !ok {
			for ii := 0; ii < len(Topic.GetPartitions()); ii++ {
				response.GetTopics()[i].Partitions = append(response.GetTopics()[i].Partitions, &broker_rpc.PullMessagesResponse_TopicResponses_PartitionResponses{
					PartitionID: Topic.GetPartitions()[ii].PartitionID,
					Ret:         broker_rpc.RetCode_NotLeader.Enum(),
				})
			}
		} else {
			for ii := 0; ii < len(Topic.GetPartitions()); ii++ {
				part, ok := pMap[Topic.GetPartitions()[ii].GetPartitionID()]
				if !ok {
					response.GetTopics()[i].Partitions = append(response.GetTopics()[i].Partitions, &broker_rpc.PullMessagesResponse_TopicResponses_PartitionResponses{
						PartitionID: Topic.GetPartitions()[ii].PartitionID,
						Ret:         broker_rpc.RetCode_NotLeader.Enum(),
					})
				} else {
					partRes := part.pullMessage(Topic.GetPartitions()[ii])
					response.GetTopics()[i].Partitions = append(response.GetTopics()[i].Partitions, partRes)
				}
			}
		}
	}
	return response, nil
}

// message CommitIndexRequest {
//     optional string ConsumerGroup = 1;
//     optional string TopicName = 2;
//     optional int32 PartitionID = 3;
//     optional uint64 CommitIndex = 4;
// }

//	message CommitIndexResponse {
//	    optional RetCode Ret = 1;
//	}
func (s *Server) CommitIndex(ctx context.Context, in *broker_rpc.CommitIndexRequest) (*broker_rpc.CommitIndexResponse, error) {
	if pMap, ok := s.topic2partition[in.GetTopicName()]; !ok {
		return &broker_rpc.CommitIndexResponse{
			Ret: broker_rpc.RetCode_NotLeader.Enum(),
		}, nil
	} else {
		part, ok := pMap[in.GetPartitionID()]
		if !ok {
			return &broker_rpc.CommitIndexResponse{
				Ret: broker_rpc.RetCode_NotLeader.Enum(),
			}, nil
		} else {
			res := part.commitIndex(in)
			return res, nil
		}
	}
}

// message GetCommitIndexRequest {
//     optional string ConsumerGroup = 1;
//     optional string TopicName = 2;
//     optional int32 PartitionID = 3;
// }

//	message GetCommitIndexResponse {
//	    optional RetCode Ret = 1;
//	    optional uint64 CommitIndex = 2;
//	}
func (s *Server) GetCommitIndex(ctx context.Context, in *broker_rpc.GetCommitIndexRequest) (*broker_rpc.GetCommitIndexResponse, error) {
	if pMap, ok := s.topic2partition[in.GetTopicName()]; !ok {
		return &broker_rpc.GetCommitIndexResponse{
			Ret: broker_rpc.RetCode_NotLeader.Enum(),
		}, nil
	} else {
		part, ok := pMap[in.GetPartitionID()]
		if !ok {
			return &broker_rpc.GetCommitIndexResponse{
				Ret: broker_rpc.RetCode_NotLeader.Enum(),
			}, nil
		} else {
			res := part.getCommitIndex(in)
			return res, nil
		}
	}
}

// message InitProducerIDRequest {
//     optional string TopicName = 1;
//     optional int32 PartitionID = 2;
// }

//	message InitProducerIDResponse {
//	    optional RetCode Ret = 1;
//	    optional uint64 ProducerID = 2;
//	}
func (s *Server) InitProducerID(ctx context.Context, in *broker_rpc.InitProducerIDRequest) (*broker_rpc.InitProducerIDResponse, error) {
	if pMap, ok := s.topic2partition[in.GetTopicName()]; !ok {
		return &broker_rpc.InitProducerIDResponse{
			Ret: broker_rpc.RetCode_NotLeader.Enum(),
		}, nil
	} else {
		part, ok := pMap[in.GetPartitionID()]
		if !ok {
			return &broker_rpc.InitProducerIDResponse{
				Ret: broker_rpc.RetCode_NotLeader.Enum(),
			}, nil
		} else {
			res := part.initProducerID(in)
			return res, nil
		}
	}
}
