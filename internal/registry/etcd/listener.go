package etcd

import (
	"strings"

	"github.com/yeyeye2333/PacificaMQ/internal/logger"
	"github.com/yeyeye2333/PacificaMQ/internal/registry/common"
	"github.com/yeyeye2333/PacificaMQ/pkg/conv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

type listener struct {
	common.Listener
	rootPath, brokerPrefix, partitionPrefix, consumerPrefix string
}

func newListener(l common.Listener, rootPath, brokerPrefix, partitionPrefix, consumerPrefix string) *listener {
	return &listener{
		Listener:        l,
		rootPath:        rootPath,
		brokerPrefix:    brokerPrefix,
		partitionPrefix: partitionPrefix,
		consumerPrefix:  consumerPrefix,
	}
}

func (l *listener) initialize(response *clientv3.GetResponse) {
	var brokerData, parData, conData, leaderData []interface{}
	for _, kv := range response.Kvs {
		k, v := string(kv.Key), string(kv.Value)
		obj, err := l.parse(k, v)
		if err != nil {
			logger.Errorf("etcd listener initialize parse error: %v", err)
		} else {
			switch data := obj.(type) {
			case *common.BrokerInfo:
				brokerData = append(brokerData, data)
			case *common.PartitionInfo:
				parData = append(parData, data)
			case *common.ConsumerInfo:
				conData = append(conData, data)
			case *common.ConsumerLeader:
				leaderData = append(leaderData, data)
			default:
				logger.Errorf("etcd listener initialize unknown data type: %v", data)
			}
		}
	}
	if len(brokerData) > 0 {
		l.Process(&common.Event{Type: common.PutBroker, Data: brokerData})
	}
	if len(parData) > 0 {
		l.Process(&common.Event{Type: common.PutPartition, Data: parData})
	}
	if len(conData) > 0 {
		l.Process(&common.Event{Type: common.PutConsumerGroup, Data: conData})
	}
	if len(leaderData) > 0 {
		l.Process(&common.Event{Type: common.PutConsumerLeader, Data: leaderData})
	}
}

func (l *listener) process(event *clientv3.Event) {
	k := string(event.Kv.Key)
	v := string(event.Kv.Value)
	data, err := l.parse(k, v)
	if err != nil {
		logger.Errorf("etcd listener process parse error: %v", err)
		return
	}

	if event.Type == clientv3.EventTypePut {
		switch data.(type) {
		case *common.BrokerInfo:
			l.Process(&common.Event{Type: common.PutBroker, Data: []interface{}{data}})
		case *common.PartitionInfo:
			l.Process(&common.Event{Type: common.PutPartition, Data: []interface{}{data}})
		case *common.ConsumerInfo:
			l.Process(&common.Event{Type: common.PutConsumerGroup, Data: []interface{}{data}})
		case *common.ConsumerLeader:
			l.Process(&common.Event{Type: common.PutConsumerLeader, Data: []interface{}{data}})
		default:
			logger.Errorf("etcd listener process unknown data type: %v", data)
		}
	} else if event.Type == clientv3.EventTypeDelete {
		switch data.(type) {
		case *common.BrokerInfo:
			l.Process(&common.Event{Type: common.DelBroker, Data: []interface{}{data}})
		case *common.PartitionInfo:
			l.Process(&common.Event{Type: common.DelPartition, Data: []interface{}{data}})
		case *common.ConsumerInfo:
			l.Process(&common.Event{Type: common.DelConsumerGroup, Data: []interface{}{data}})
		case *common.ConsumerLeader:
			l.Process(&common.Event{Type: common.DelConsumerLeader, Data: []interface{}{data}})
		default:
			logger.Errorf("etcd listener process unknown data type: %v", data)
		}
	}
}

// 内部方法
func (l *listener) parse(k, v string) (interface{}, error) {
	strs := strings.Split(k[len(l.rootPath):], "/")
	switch {
	case strings.HasPrefix(k, l.brokerPrefix):
		brokerInfo := &common.BrokerInfo{
			Address:       strs[2],
			NewPartitions: &common.OwnPartitions{},
		}
		if v != "" {
			err := proto.Unmarshal([]byte(v), brokerInfo.NewPartitions)
			if err != nil {
				return nil, err
			}
		}
		return brokerInfo, nil
	case strings.HasPrefix(k, l.partitionPrefix):
		parInfo := &common.PartitionInfo{
			TopicName:   strs[2],
			PartitionID: conv.BytesToInt32([]byte(strs[3])),
			Status:      &common.PartitionStatus{},
		}
		if v != "" {
			err := proto.Unmarshal([]byte(v), parInfo.Status)
			if err != nil {
				return nil, err
			}
		}
		return parInfo, nil

	case strings.HasPrefix(k, l.consumerPrefix):
		if strs[2] == "ids" {
			conInfo := &common.ConsumerInfo{
				GroupID:         strs[1],
				ConsumerAddress: strs[3],
				SubList:         &common.SubList{},
			}
			if v != "" {
				err := proto.Unmarshal([]byte(v), conInfo.SubList)
				if err != nil {
					return nil, err
				}
			}
			return conInfo, nil
		} else {
			conLeader := &common.ConsumerLeader{
				GroupID: strs[1],
				Leader:  v,
			}
			return conLeader, nil
		}

	default:
		return nil, common.ErrNotSupport
	}
}
