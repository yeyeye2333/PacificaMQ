package common

import (
	proto "github.com/yeyeye2333/PacificaMQ/pacifica/api"
)

// 需要线程安全
type Storage interface {
	Start() error
	Close() error

	Load(index uint64) (*proto.Entry, error)
	LoadMin() (*proto.Entry, error)
	LoadMax() (*proto.Entry, error)
	MoreLoad(index uint64, num uint64) ([]*proto.Entry, error)
	Save(entrys []*proto.Entry) error
	Release(begin uint64, end uint64) error

	SetSync(isSync bool)
}
