package client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"

	"github.com/yeyeye2333/PacificaMQ/api/broker_rpc"
	"github.com/yeyeye2333/PacificaMQ/api/consumer_rebalance"
	"github.com/yeyeye2333/PacificaMQ/internal/registry"
	regCM "github.com/yeyeye2333/PacificaMQ/internal/registry/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 分区函数和拦截器函数定义
type Interceptor func([]byte) []byte
type Partitioner func(topic string, data []byte) int32

type Client struct {
	opts                    *Options
	address                 string
	maxTimeOut              int64
	brokerCliLock           sync.RWMutex
	broker2Connection       map[string]*grpc.ClientConn
	broker2Cli              map[string]broker_rpc.BrokerClient
	registry                regCM.Registry
	rwlock                  sync.RWMutex
	topic2Partition2Leader  map[string]map[int32]string
	topic2Partition2Brokers map[string]map[int32][]string
	broker2Topic2Partitions map[string]map[string][]int32

	// producer
	producerLock               sync.RWMutex
	topic2Partition2producerID map[string]map[int32]uint64
	topic2Partition2Cache      map[string]map[int32][][]byte
	broker2Sender              map[string]context.CancelFunc
	sequence                   uint64
	interceptors               Interceptor
	partitioner                Partitioner

	// consumer
	consumer_rebalance.UnimplementedConsumerRebalanceServer
	consumerServer  *grpc.Server
	consumerGroup   string
	consumerLock    sync.RWMutex
	leader          string
	subList         regCM.SubList
	cond            *sync.Cond
	stop            bool
	topics          []string
	partitions      []int32
	consumer2Topics map[string][]string
}

func NewClient(opts *Options) (*Client, error) {
	c := &Client{
		opts:                       opts,
		address:                    opts.Address,
		maxTimeOut:                 opts.MaxTimeOut,
		topic2Partition2Leader:     make(map[string]map[int32]string),
		topic2Partition2Brokers:    make(map[string]map[int32][]string),
		broker2Topic2Partitions:    make(map[string]map[string][]int32),
		broker2Connection:          make(map[string]*grpc.ClientConn),
		broker2Cli:                 make(map[string]broker_rpc.BrokerClient),
		topic2Partition2producerID: make(map[string]map[int32]uint64),
		topic2Partition2Cache:      make(map[string]map[int32][][]byte),
		broker2Sender:              make(map[string]context.CancelFunc),
		sequence:                   0,
		topics:                     make([]string, 0),
		partitions:                 make([]int32, 0),
		consumer2Topics:            make(map[string][]string),
		interceptors:               opts.Interceptor,
		partitioner:                opts.Partitioner,
	}
	if c.partitioner == nil {
		c.partitioner = c.myPartitioner
	}
	c.cond = sync.NewCond(&c.consumerLock)
	c.registry, _ = registry.NewRegistry(regCM.NewOptions(regCM.WithAddress(opts.EtcdAddress)))

	lis, err := net.Listen("tcp", c.address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	c.consumerServer = grpc.NewServer()
	consumer_rebalance.RegisterConsumerRebalanceServer(c.consumerServer, c)
	go c.consumerServer.Serve(lis)
	c.registry.SubBroker("", &addressSub{c})

	return c, nil
}

func (c *Client) myPartitioner(topic string, data []byte) int32 {
	c.rwlock.RLock()
	defer c.rwlock.RUnlock()

	partitions, ok := c.topic2Partition2Brokers[topic]
	if !ok || len(partitions) == 0 {
		return 1
	}

	partitionCount := int32(len(partitions))
	return int32(c.sequence) % partitionCount
}

func (c *Client) Close() error {
	c.consumerServer.Stop()
	c.registry.Close()
	for _, conn := range c.broker2Connection {
		conn.Close()
	}
	return nil
}

func (c *Client) getBrokerClient(broker string) (broker_rpc.BrokerClient, error) {
	c.brokerCliLock.RLock()
	if cli, ok := c.broker2Cli[broker]; ok {
		c.brokerCliLock.RUnlock()
		return cli, nil
	}
	c.brokerCliLock.RUnlock()
	c.brokerCliLock.Lock()
	defer c.brokerCliLock.Unlock()
	if cli, ok := c.broker2Cli[broker]; ok {
		return cli, nil
	}
	conn, err := grpc.NewClient("broker", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.broker2Connection[broker] = conn
	c.broker2Cli[broker] = broker_rpc.NewBrokerClient(conn)
	return c.broker2Cli[broker], nil
}

// 生产者相关
func (c *Client) getProducerID(topic string, partitionID int32) (uint64, error) {
	c.producerLock.RLock()
	if _, ok := c.topic2Partition2producerID[topic]; !ok {
		c.producerLock.RUnlock()
		c.producerLock.Lock()
		defer c.producerLock.Unlock()
		if _, ok := c.topic2Partition2producerID[topic]; !ok {
			c.topic2Partition2producerID[topic] = make(map[int32]uint64)
		}
	}
	if _, ok := c.topic2Partition2producerID[topic][partitionID]; !ok {
		c.producerLock.RUnlock()

		// Get leader broker for this partition
		c.rwlock.RLock()
		leader, ok := c.topic2Partition2Leader[topic][partitionID]
		c.rwlock.RUnlock()
		if !ok {
			return 0, errors.New("no leader available for partition")
		}

		req := &broker_rpc.InitProducerIDRequest{
			TopicName:   &topic,
			PartitionID: &partitionID,
		}
		cli, err := c.getBrokerClient(leader)
		if err != nil {
			return 0, err
		}
		resp, err := cli.InitProducerID(context.Background(), req)
		if err != nil {
			return 0, err
		}
		c.producerLock.Lock()
		defer c.producerLock.Unlock()
		if _, ok := c.topic2Partition2producerID[topic][partitionID]; !ok {
			c.topic2Partition2producerID[topic][partitionID] = resp.GetProducerID()
		}
	}
	producerID := c.topic2Partition2producerID[topic][partitionID]
	c.producerLock.RUnlock()
	return producerID, nil
}

func (c *Client) sendMessages(ctx context.Context, broker string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			cli, err := c.getBrokerClient(broker)
			if err != nil {
				continue
			}

			stream, err := cli.PushMessages(ctx)
			if err != nil {
				continue
			}

			c.producerLock.RLock()
			topics := make([]*broker_rpc.PushMessagesRequest_Topic, 0)
			for topic, partitions := range c.broker2Topic2Partitions[broker] {
				topicReq := &broker_rpc.PushMessagesRequest_Topic{
					TopicName: &topic,
				}
				for _, partitionID := range partitions {
					if messages, ok := c.topic2Partition2Cache[topic][partitionID]; ok && len(messages) > 0 {
						partitionReq := &broker_rpc.PushMessagesRequest_Topic_Partition{
							PartitionID: &partitionID,
						}
						producerID, err := c.getProducerID(topic, partitionID)
						if err != nil {
							continue
						}
						partitionReq.ProducerID = &producerID
						partitionReq.SequenceNumber = &c.sequence
						c.sequence++
						partitionReq.Datas = messages
						topicReq.Partitions = append(topicReq.Partitions, partitionReq)
					}
				}
				if len(topicReq.Partitions) > 0 {
					topics = append(topics, topicReq)
				}
			}
			c.producerLock.RUnlock()

			if len(topics) == 0 {
				continue
			}

			req := &broker_rpc.PushMessagesRequest{
				Topics: topics,
			}
			if err := stream.Send(req); err != nil {
				continue
			}

			resp, err := stream.Recv()
			if err != nil {
				continue
			}

			for _, topicResp := range resp.GetTopics() {
				for _, partitionResp := range topicResp.GetPartitions() {
					if partitionResp.GetRet() == broker_rpc.RetCode_SUCCESS {
						c.producerLock.Lock()
						if messages, ok := c.topic2Partition2Cache[topicResp.GetTopicName()][partitionResp.GetPartitionID()]; ok {
							if int(partitionResp.GetStartIndex()) <= len(messages) {
								c.topic2Partition2Cache[topicResp.GetTopicName()][partitionResp.GetPartitionID()] =
									messages[partitionResp.GetStartIndex():]
							}
						}
						c.producerLock.Unlock()
					}
				}
			}
		}
	}
}

func (c *Client) Send(topic string, message []byte) {
	if c.interceptors != nil {
		message = c.interceptors(message)
	}

	var partitionID int32
	if c.partitioner != nil {
		partitionID = c.partitioner(topic, message)
	} else {
		partitionID = 0
	}

	c.producerLock.Lock()
	defer c.producerLock.Unlock()

	if _, ok := c.topic2Partition2Cache[topic]; !ok {
		c.topic2Partition2Cache[topic] = make(map[int32][][]byte)
	}
	if _, ok := c.topic2Partition2Cache[topic][partitionID]; !ok {
		c.topic2Partition2Cache[topic][partitionID] = make([][]byte, 0)
	}
	c.topic2Partition2Cache[topic][partitionID] = append(c.topic2Partition2Cache[topic][partitionID], message)

	leader, ok := c.topic2Partition2Leader[topic][partitionID]
	if !ok {
		return
	}

	if _, ok := c.broker2Sender[leader]; !ok {
		ctx, cancel := context.WithCancel(context.Background())
		c.broker2Sender[leader] = cancel
		go c.sendMessages(ctx, leader)
	}
}

func (c *Client) GetCommitIndex(topic string, partitionID int32) (uint64, error) {
	c.rwlock.RLock()
	leader, ok := c.topic2Partition2Leader[topic][partitionID]
	c.rwlock.RUnlock()
	if !ok {
		return 0, errors.New("no leader available for partition")
	}

	req := &broker_rpc.GetCommitIndexRequest{
		TopicName:     &topic,
		PartitionID:   &partitionID,
		ConsumerGroup: &c.consumerGroup,
	}
	cli, err := c.getBrokerClient(leader)
	if err != nil {
		return 0, err
	}
	resp, err := cli.GetCommitIndex(context.Background(), req)
	if err != nil {
		return 0, err
	}
	return resp.GetCommitIndex(), nil
}

func (c *Client) CommitIndex(topic string, partitionID int32, commitIndex uint64) error {
	c.rwlock.RLock()
	leader, ok := c.topic2Partition2Leader[topic][partitionID]
	c.rwlock.RUnlock()
	if !ok {
		return errors.New("no leader available for partition")
	}

	req := &broker_rpc.CommitIndexRequest{
		TopicName:     &topic,
		PartitionID:   &partitionID,
		ConsumerGroup: &c.consumerGroup,
		CommitIndex:   &commitIndex,
	}
	cli, err := c.getBrokerClient(leader)
	if err != nil {
		return err
	}
	resp, err := cli.CommitIndex(context.Background(), req)
	if err != nil {
		return err
	}
	if resp.GetRet() != broker_rpc.RetCode_SUCCESS {
		return errors.New(resp.GetRet().String())
	}
	return nil
}

func (c *Client) JoinConsumerGroup(consumerGroup string) error {
	c.consumerGroup = consumerGroup
	c.registry.Register(&regCM.ConsumerInfo{
		GroupID:         consumerGroup,
		ConsumerAddress: c.address,
	})
	c.registry.SubConsumerLeader(consumerGroup, &consumerGroupLeaderSub{c})
	c.registry.GetConsumerLeader(&regCM.ConsumerLeader{
		GroupID: consumerGroup,
		Leader:  c.address,
	})
	return nil
}

func (c *Client) SubTopic(topic string) error {
	c.consumerLock.Lock()
	defer c.consumerLock.Unlock()
	c.subList.SubTopics = append(c.subList.SubTopics, topic)
	c.registry.Register(&regCM.ConsumerInfo{
		GroupID:         c.consumerGroup,
		ConsumerAddress: c.address,
		SubList:         &c.subList,
	})
	return nil
}

type consumedData struct {
	topic      string
	partition  int32
	startindex uint64
	data       [][]byte
}

func (c *Client) Poll(ctx context.Context, MaxBytes uint32) chan *consumedData {
	c.consumerLock.Lock()
	defer c.consumerLock.Unlock()

	ch := make(chan *consumedData, 100)

	type worker struct {
		ctx    context.Context
		cancel context.CancelFunc
	}
	workers := make(map[string]map[int32]*worker) //topic->partition->worker

	startWorker := func(topic string, partition int32) {
		c.consumerLock.RLock()
		leader, ok := c.topic2Partition2Leader[topic][partition]
		c.consumerLock.RUnlock()

		if !ok {
			return
		}

		workerCtx, cancel := context.WithCancel(ctx)
		if _, ok := workers[topic]; !ok {
			workers[topic] = make(map[int32]*worker)
		}
		workers[topic][partition] = &worker{
			ctx:    workerCtx,
			cancel: cancel,
		}

		go func() {
			defer func() {
				c.consumerLock.Lock()
				delete(workers[topic], partition)
				if len(workers[topic]) == 0 {
					delete(workers, topic)
				}
				c.consumerLock.Unlock()
			}()

			for {
				select {
				case <-workerCtx.Done():
					return
				default:
					req := &broker_rpc.PullMessagesRequest{
						Topics: []*broker_rpc.PullMessagesRequest_Topic{
							{
								TopicName: &topic,
								Partitions: []*broker_rpc.PullMessagesRequest_Topic_Partition{
									{
										PartitionID: &partition,
										FetchIndex:  &c.sequence,
										MaxBytes:    &MaxBytes,
									},
								},
							},
						},
					}
					cli, err := c.getBrokerClient(leader)
					if err != nil {
						continue
					}
					resp, err := cli.PullMessages(workerCtx, req)
					if err != nil {
						continue
					}

					for _, topicResp := range resp.GetTopics() {
						for _, partitionResp := range topicResp.GetPartitions() {
							if partitionResp.GetRet() == broker_rpc.RetCode_SUCCESS {
								for _, data := range partitionResp.GetDatas() {
									if c.interceptors != nil {
										data = c.interceptors(data)
									}
									select {
									case ch <- &consumedData{
										topic:      topicResp.GetTopicName(),
										partition:  partitionResp.GetPartitionID(),
										startindex: *req.Topics[0].Partitions[0].FetchIndex,
										data:       [][]byte{data},
									}:
									case <-workerCtx.Done():
										return
									}
								}
							}
						}
					}
				}
			}
		}()
	}

	go func() {
		defer close(ch)
		defer func() {
			for _, topicWorkers := range workers {
				for _, w := range topicWorkers {
					w.cancel()
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				c.consumerLock.Lock()
				if c.stop {
					c.consumerLock.Unlock()
					c.cond.Wait()
					continue
				}
				activePartitions := make(map[string]map[int32]struct{})
				for _, topic := range c.subList.SubTopics {
					if partMap, ok := c.topic2Partition2Brokers[topic]; ok {
						activePartitions[topic] = make(map[int32]struct{})
						for partID := range partMap {
							activePartitions[topic][partID] = struct{}{}
						}
					}
				}
				c.consumerLock.Unlock()

				for topic, partitions := range activePartitions {
					for partID := range partitions {
						c.consumerLock.Lock()
						if _, ok := workers[topic]; !ok {
							workers[topic] = make(map[int32]*worker)
						}
						if _, ok := workers[topic][partID]; !ok {
							startWorker(topic, partID)
						}
						c.consumerLock.Unlock()
					}
				}
				c.consumerLock.Lock()
				for topic, topicWorkers := range workers {
					if _, ok := activePartitions[topic]; !ok {
						for partID, w := range topicWorkers {
							w.cancel()
							delete(topicWorkers, partID)
						}
						continue
					}
					for partID, w := range topicWorkers {
						if _, ok := activePartitions[topic][partID]; !ok {
							w.cancel()
							delete(topicWorkers, partID)
						}
					}
				}
				c.consumerLock.Unlock()
				c.cond.Wait()
			}
		}
	}()

	return ch
}

func (c *Client) Rebalance(ctx context.Context, in *consumer_rebalance.RebalanceRequest) (*consumer_rebalance.RebalanceResponse, error) {
	c.consumerLock.Lock()
	defer c.consumerLock.Unlock()
	c.topics = in.Topics
	c.partitions = in.PartitionIDs
	c.stop = true
	return &consumer_rebalance.RebalanceResponse{}, nil
}

func (c *Client) Commit(ctx context.Context, in *consumer_rebalance.CommitRequest) (*consumer_rebalance.CommitResponse, error) {
	c.consumerLock.Lock()
	defer c.consumerLock.Unlock()
	if len(c.topics) != len(in.Topics) {
		return nil, errors.New("topics not match")
	}
	for index := range in.Topics {
		if c.topics[index] != in.Topics[index] || c.partitions[index] != in.PartitionIDs[index] {
			return nil, errors.New("topics or partitions not match")
		}
	}
	c.stop = false
	c.cond.Broadcast()
	return &consumer_rebalance.CommitResponse{}, nil
}

// admin相关
func (c *Client) ListPartitions() (map[string]int32, error) {
	c.rwlock.RLock()
	defer c.rwlock.RUnlock()
	result := make(map[string]int32)
	for topic, partitions := range c.topic2Partition2Brokers {
		result[topic] = int32(len(partitions))
	}
	return result, nil
}

func (c *Client) CreatePartition(topic string, partitionNum int32, replicaNum int32) error {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	if len(topic) == 0 {
		return errors.New("topic name cannot be empty")
	}
	if partitionNum <= 0 {
		return errors.New("partition number must be positive")
	}
	if replicaNum <= 0 {
		return errors.New("replica number must be positive")
	}

	if _, ok := c.topic2Partition2Brokers[topic]; ok {
		return errors.New("topic already exists")
	}
	brokerCount := int32(len(c.broker2Topic2Partitions))
	if brokerCount == 0 {
		return errors.New("no available brokers")
	}
	if replicaNum > brokerCount {
		return fmt.Errorf("replica number %d exceeds available broker count %d", replicaNum, brokerCount)
	}
	c.topic2Partition2Brokers[topic] = make(map[int32][]string)
	c.topic2Partition2Leader[topic] = make(map[int32]string)
	brokerLoad := make(map[string]int)
	for broker, topicParts := range c.broker2Topic2Partitions {
		brokerLoad[broker] = 0
		for _, parts := range topicParts {
			brokerLoad[broker] += len(parts)
		}
	}

	type brokerInfo struct {
		addr string
		load int
	}
	var sortedBrokers []brokerInfo
	for addr, load := range brokerLoad {
		sortedBrokers = append(sortedBrokers, brokerInfo{addr, load})
	}
	sort.Slice(sortedBrokers, func(i, j int) bool {
		return sortedBrokers[i].load < sortedBrokers[j].load
	})

	for i := int32(0); i < partitionNum; i++ {
		replicas := make([]string, 0, replicaNum)
		for j := 0; j < int(replicaNum); j++ {
			selected := sortedBrokers[j%len(sortedBrokers)].addr
			replicas = append(replicas, selected)
		}
		c.topic2Partition2Brokers[topic][i] = replicas
		c.topic2Partition2Leader[topic][i] = replicas[0]

		for _, broker := range replicas {
			if _, ok := c.broker2Topic2Partitions[broker]; !ok {
				c.broker2Topic2Partitions[broker] = make(map[string][]int32)
			}
			c.broker2Topic2Partitions[broker][topic] = append(c.broker2Topic2Partitions[broker][topic], i)
		}
	}

	return nil
}

// 订阅地址
type addressSub struct {
	*Client
}

func (c *addressSub) Process(event *regCM.Event) {
	switch event.Type {
	case regCM.PutBroker:
		c.rwlock.Lock()
		defer c.rwlock.Unlock()
		brokerInfo := event.Data[0].(*regCM.BrokerInfo)
		if _, ok := c.broker2Topic2Partitions[brokerInfo.Address]; !ok {
			c.broker2Topic2Partitions[brokerInfo.Address] = make(map[string][]int32)
		}
		for index := range brokerInfo.NewPartitions.TopicName {
			topic := brokerInfo.NewPartitions.TopicName[index]
			partID := brokerInfo.NewPartitions.PartitionNum[index]
			c.broker2Topic2Partitions[brokerInfo.Address][topic] = append(c.broker2Topic2Partitions[brokerInfo.Address][topic], partID)
			if _, ok := c.topic2Partition2Brokers[topic]; !ok {
				c.topic2Partition2Brokers[topic] = make(map[int32][]string)
			}
			if _, ok := c.topic2Partition2Brokers[topic][partID]; !ok {
				c.topic2Partition2Brokers[topic][partID] = make([]string, 0)
			}
			exist := false
			for _, addr := range c.topic2Partition2Brokers[topic][partID] {
				if addr == brokerInfo.Address {
					exist = true
					break
				}
			}
			if !exist {
				c.topic2Partition2Brokers[topic][partID] = append(c.topic2Partition2Brokers[topic][partID], brokerInfo.Address)
			}
			if _, ok := c.topic2Partition2Leader[topic]; !ok {
				c.topic2Partition2Leader[topic] = make(map[int32]string)
			}
			if _, ok := c.topic2Partition2Leader[topic][partID]; !ok {
				//随机选一个leader
				c.topic2Partition2Leader[topic][partID] = c.topic2Partition2Brokers[topic][partID][0]
			}
		}
	case regCM.DelBroker:
		c.rwlock.Lock()
		defer c.rwlock.Unlock()
		brokerInfo := event.Data[0].(*regCM.BrokerInfo)
		if _, ok := c.broker2Topic2Partitions[brokerInfo.Address]; !ok {
			return
		}
		for index := range brokerInfo.OldPartitions.TopicName {
			topic := brokerInfo.OldPartitions.TopicName[index]
			partID := brokerInfo.OldPartitions.PartitionNum[index]
			if _, ok := c.topic2Partition2Brokers[topic]; !ok {
				continue
			}
			if _, ok := c.topic2Partition2Brokers[topic][partID]; !ok {
				continue
			}
			for i, addr := range c.topic2Partition2Brokers[topic][partID] {
				if addr == brokerInfo.Address {
					c.topic2Partition2Brokers[topic][partID] = append(c.topic2Partition2Brokers[topic][partID][:i], c.topic2Partition2Brokers[topic][partID][i+1:]...)
					break
				}
			}
			if len(c.topic2Partition2Brokers[topic][partID]) == 0 {
				delete(c.topic2Partition2Brokers[topic], partID)
			}
			if leader, ok := c.topic2Partition2Leader[topic][partID]; ok && leader == brokerInfo.Address {
				if len(c.topic2Partition2Brokers[topic][partID]) > 0 {
					c.topic2Partition2Leader[topic][partID] = c.topic2Partition2Brokers[topic][partID][0]
				} else {
					delete(c.topic2Partition2Leader[topic], partID)
				}
			}
			if len(c.topic2Partition2Brokers[topic]) == 0 {
				delete(c.topic2Partition2Brokers, topic)
			}
		}
		delete(c.broker2Topic2Partitions, brokerInfo.Address)
	}
}

// 订阅消费者组
type consumerGroupSub struct {
	*Client
}

func (c *consumerGroupSub) Process(event *regCM.Event) {
	c.consumerLock.Lock()
	defer c.consumerLock.Unlock()

	switch event.Type {
	case regCM.PutConsumerGroup:
		for _, data := range event.Data {
			consumerInfo := data.(*regCM.ConsumerInfo)
			c.consumer2Topics[consumerInfo.ConsumerAddress] = consumerInfo.SubList.SubTopics
		}
	case regCM.DelConsumerGroup:
		for _, data := range event.Data {
			consumerInfo := data.(*regCM.ConsumerInfo)
			delete(c.consumer2Topics, consumerInfo.ConsumerAddress)
		}
	}
	topicPartitions := make(map[string][]int32)
	for topic := range c.topic2Partition2Leader {
		for partition := range c.topic2Partition2Leader[topic] {
			topicPartitions[topic] = append(topicPartitions[topic], partition)
		}
	}

	// 轮询分配分区给消费者
	assignments := make(map[string]*consumer_rebalance.RebalanceRequest)
	for topic, partitions := range topicPartitions {
		var consumers []string
		for consumer, topics := range c.consumer2Topics {
			for _, t := range topics {
				if t == topic {
					consumers = append(consumers, consumer)
					break
				}
			}
		}
		if len(consumers) == 0 {
			continue
		}
		for i, partition := range partitions {
			consumer := consumers[i%len(consumers)]
			if _, ok := assignments[consumer]; !ok {
				assignments[consumer] = &consumer_rebalance.RebalanceRequest{
					Topics:       []string{},
					PartitionIDs: []int32{},
				}
			}
			assignments[consumer].Topics = append(assignments[consumer].Topics, topic)
			assignments[consumer].PartitionIDs = append(assignments[consumer].PartitionIDs, partition)
		}
	}

	// 第一阶段：发送所有重平衡请求
	type rebalanceResult struct {
		addr    string
		req     *consumer_rebalance.RebalanceRequest
		success bool
		err     error
	}
	results := make(chan rebalanceResult, len(assignments))
	var wg sync.WaitGroup
	for consumer, assignment := range assignments {
		wg.Add(1)
		go func(addr string, req *consumer_rebalance.RebalanceRequest) {
			defer wg.Done()
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				results <- rebalanceResult{addr: addr, req: req, success: false, err: err}
				return
			}
			defer conn.Close()
			cli := consumer_rebalance.NewConsumerRebalanceClient(conn)
			_, err = cli.Rebalance(context.Background(), req)
			results <- rebalanceResult{addr: addr, req: req, success: err == nil, err: err}
		}(consumer, assignment)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	allSucceeded := true
	var failedResults []rebalanceResult
	for result := range results {
		if !result.success {
			allSucceeded = false
			failedResults = append(failedResults, result)
		}
	}

	// 只有所有重平衡成功才发送commit
	if allSucceeded {
		for consumer, assignment := range assignments {
			wg.Add(1)
			go func(addr string, req *consumer_rebalance.RebalanceRequest) {
				defer wg.Done()
				conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return
				}
				defer conn.Close()
				cli := consumer_rebalance.NewConsumerRebalanceClient(conn)
				_, _ = cli.Commit(context.Background(), &consumer_rebalance.CommitRequest{
					Topics:       req.Topics,
					PartitionIDs: req.PartitionIDs,
				})
			}(consumer, assignment)
		}
		wg.Wait()
	} else {
		for _, failed := range failedResults {
			log.Printf("Rebalance failed for consumer %s: %v", failed.addr, failed.err)
		}
	}
}

// 订阅消费者组Leader
type consumerGroupLeaderSub struct {
	*Client
}

func (c *consumerGroupLeaderSub) Process(event *regCM.Event) {
	switch event.Type {
	case regCM.DelConsumerLeader:
		c.rwlock.Lock()
		defer c.rwlock.Unlock()
		err := c.registry.GetConsumerLeader(&regCM.ConsumerLeader{
			GroupID: c.consumerGroup,
			Leader:  c.address,
		})
		if err == nil {
			c.registry.SubConsumerGroup(c.consumerGroup, &consumerGroupSub{c.Client})
		}
	case regCM.PutConsumerLeader:
		c.rwlock.Lock()
		defer c.rwlock.Unlock()
		leader := event.Data[0].(*regCM.ConsumerLeader).Leader
		if leader == c.address {
			c.registry.UnSubConsumerGroup(c.consumerGroup)
		}
		c.leader = leader
	}
}
