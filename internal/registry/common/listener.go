package common

type Listener interface {
	Process(*Event)
}

type EventType int

const (
	PutBroker EventType = iota
	DelBroker

	PutPartition
	DelPartition

	PutConsumerGroup
	DelConsumerGroup

	PutConsumerLeader
	DelConsumerLeader
)

type Event struct {
	Type EventType
	Data []interface{} //指针类型
}
