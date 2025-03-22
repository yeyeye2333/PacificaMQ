package common

import (
	"github.com/yeyeye2333/PacificaMQ/pacifica/api"
)

// 需要线程安全
type Storage interface {
	Start() error
	Close() error

	Load(index uint64, num uint32) (string, error)
	Save(entrys []api.Entry) error

	SetSync(isSync bool)
}
