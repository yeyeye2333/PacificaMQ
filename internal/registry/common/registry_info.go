package common

import (
	"github.com/yeyeye2333/PacificaMQ/api/registry_info"
)

type PartitionStatus = registry_info.PartitionStatus
type SubList = registry_info.SubList

type PartitionInfo struct {
	TopicName   string
	PartitionID int32
	Status      PartitionStatus
}

type ConsumerInfo struct {
	GroupID         string
	ConsumerAddress string
	SubList         SubList
}

type ConsumerLeader struct {
	GroupID string
	Leader  string
}
