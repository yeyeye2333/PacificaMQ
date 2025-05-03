package common

type Registry interface {
	Close() error

	Register(interface{}) error // 注册PartitionInfo 或 ConsumerInfo
	UnRegister(interface{}) error

	SubPartition(topic string, listener Listener) error // topic/
	UnSubPartition(topic string)
	SubConsumerGroup(groupID string, listener Listener) error //groupID/ids/
	UnSubConsumerGroup(groupID string)

	//Broker拥有分区相关
	ChangeBroker(brokerInfo *BrokerInfo) error // broker修改自身拥有分区

	SubBroker(broker string, listener Listener) error // broker
	UnSubBroker(broker string)
	//消费者组选主相关
	GetConsumerLeader(me *ConsumerLeader) error // 竞争成为消费者组Leader

	SubConsumerLeader(groupID string, listener Listener) error // groupID/leader
	UnSubConsumerLeader(groupID string)
}
