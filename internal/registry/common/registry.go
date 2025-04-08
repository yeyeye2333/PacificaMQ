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

	//选主相关
	GetConsumerLeader(me ConsumerLeader) error

	SubConsumerLeader(groupID string, listener Listener) error
	UnSubConsumerLeader(groupID string)
}
