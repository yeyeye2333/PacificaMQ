package storage

import (
	"github.com/yeyeye2333/PacificaMQ/pacifica/api"
)

// 需要线程安全
type Storage interface {
	Close()

	Save(entrys []*api.Entry) error
	Release(begin uint64, end uint64) error
	Load(index uint64) (*api.Entry, error)
	LoadMin() (*api.Entry, error)
	LoadMax() (*api.Entry, error)
	MoreLoad(index uint64, num uint64) ([]*api.Entry, error)
}
