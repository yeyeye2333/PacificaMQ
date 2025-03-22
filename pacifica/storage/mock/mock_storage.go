package storage

import (
	"github.com/yeyeye2333/PacificaMQ/pacifica/api"
	"github.com/yeyeye2333/PacificaMQ/pacifica/extension"
	"github.com/yeyeye2333/PacificaMQ/pacifica/storage/common"
)

func init() {
	extension.SetStorage("mock", NewStorage)
}

var NewStorage extension.StorageFactory = func(opts common.InternalOptions) (common.Storage, error) {
	return &MockStorage{}, nil

}

type MockStorage struct {
}

func (s *MockStorage) Start() error {
	return nil
}

func (s *MockStorage) Close() error {
	return nil
}

func (s *MockStorage) Load(index uint64, num uint32) (string, error) {
	return "", nil
}

func (s *MockStorage) Save(entrys []api.Entry) error {
	return nil
}

func (s *MockStorage) SetSync(isSync bool) {
}
