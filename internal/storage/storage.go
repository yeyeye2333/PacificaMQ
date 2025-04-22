package storage

import (
	"github.com/yeyeye2333/PacificaMQ/api/storage_info"
)

type Storage interface {
	AppendMessages(msgs [][]byte, producerID *storage_info.ProducerID, lastPacificaIndex uint64) <-chan uint64
	SetProducerID(producerID *storage_info.ProducerID, LastPacificaIndex uint64) error
	GetMessage(beginIndex uint64, maxBytes uint32) ([]*storage_info.Record, error)
	GetProducerIDs() (map[uint64]uint64, error)
	GetLastPacificaIndex() (uint64, error)

	CommitIndex(Index *storage_info.ConsumerCommitIndex, LastPacificaIndex uint64) error
	GetCommitedIndex(GroupID string) (uint64, error)
}
