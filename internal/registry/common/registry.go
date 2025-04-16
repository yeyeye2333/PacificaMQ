package common

import (
	"github.com/yeyeye2333/PacificaMQ/common"
)

type Registry interface {
	common.Node
	Register(interface{}) error
	UnRegister(interface{}) error

	SubPartition(topic string, listener Listener) error
	UnSubPartition(topic string)
	SubConsumerGroup(groupID string, listener Listener) error
	UnSubConsumerGroup(groupID string)

	//Broker拥有分区相关
	ChangeBroker(brokerInfo *BrokerInfo) error

	SubBroker(broker string, listener Listener) error
	UnSubBroker(broker string)
	//消费者组选主相关
	GetConsumerLeader(me *ConsumerLeader) error

	SubConsumerLeader(groupID string, listener Listener) error
	UnSubConsumerLeader(groupID string)
}
