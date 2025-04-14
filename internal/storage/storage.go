package storage

import (
	"github.com/yeyeye2333/PacificaMQ/api/storage_info"
)

type Storage interface {
	AppendMessages(msgs *storage_info.ReplicativeData, producerID *storage_info.ProducerID, LastPacificaIndex uint64) (uint64, error)
	GetMessage(beginIndex uint64, maxBytes uint32) ([]*storage_info.Record, error)
	GetProducerID(id uint64) (*storage_info.ProducerID, error)
	GetLastPacificaIndex() (uint64, error)

	CommitIndex(CommitIndex uint64) error
	GetCommitedIndex() uint64

	Close()
}
