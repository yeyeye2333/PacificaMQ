package etcd

import (
	"testing"
	"time"

	"github.com/yeyeye2333/PacificaMQ/internal/logger"
	"github.com/yeyeye2333/PacificaMQ/internal/registry/common"
)

func TestBroker(t *testing.T) {
	registry, err := NewRegistry(common.NewOptions().Internal)
	if err != nil {
		t.Fatal(err)
	}
	broker1 := "broker1"
	lis := &broker_lis{}
	err = registry.SubBroker(broker1, lis)
	if err != nil {
		t.Fatal(err)
	}
	err = registry.ChangeBroker(&common.BrokerInfo{
		Address: broker1,
		NewPartitions: &common.OwnPartitions{
			TopicName:    []string{"topic1"},
			PartitionNum: []int32{1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = registry.ChangeBroker(&common.BrokerInfo{
		Address: broker1,
		OldPartitions: &common.OwnPartitions{
			TopicName:    []string{"topic1"},
			PartitionNum: []int32{1},
		},
		NewPartitions: &common.OwnPartitions{
			TopicName:    []string{"topic1", "topic2"},
			PartitionNum: []int32{1, 2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = registry.ChangeBroker(&common.BrokerInfo{
		Address: broker1,
		OldPartitions: &common.OwnPartitions{
			TopicName:    []string{"topic1", "topic2"},
			PartitionNum: []int32{1, 2},
		},
		NewPartitions: &common.OwnPartitions{},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Second)
	if lis.getNum != 3 {
		t.Error("broker listener not called")
	} else {
		t.Log("broker listener called")
	}

	err = registry.Close()
	if err != nil {
		t.Fatal(err)
	}
}

type broker_lis struct {
	getNum int
}

func (l *broker_lis) Process(event *common.Event) {
	data := event.Data[0].(*common.BrokerInfo)
	l.getNum++
	if event.Type == common.PutBroker {
		logger.Info(data.Address, data.NewPartitions)
	} else {
		logger.Info(data.Address)
	}
}
