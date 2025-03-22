package extension

import (
	"fmt"

	storage "github.com/yeyeye2333/PacificaMQ/pacifica/storage/common"
)

type StorageFactory = func(storage.InternalOptions) (storage.Storage, error)

var storages = make(map[string]StorageFactory)

func SetStorage(name string, sf StorageFactory) {
	storages[name] = sf
}

func GetStorage(name string, options storage.InternalOptions) (storage.Storage, error) {
	if sf, ok := storages[name]; ok {
		return sf(options)
	}
	return nil, fmt.Errorf("storage %s not found", name)
}
