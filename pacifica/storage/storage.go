package storage

import (
	"github.com/yeyeye2333/PacificaMQ/pacifica/extension"
	"github.com/yeyeye2333/PacificaMQ/pacifica/storage/common"
)

func NewStorage(opts ...common.Option) (common.Storage, error) {
	options := common.NewOptions(opts...)
	return extension.GetStorage(options.Name, options.Internal)
}
