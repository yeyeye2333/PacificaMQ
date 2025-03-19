package config_center

import (
	"github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
	"github.com/yeyeye2333/PacificaMQ/pacifica/extension"
)

func NewConfigCenter(opts ...common.Option) (common.ConfigCenter, error) {
	options := common.NewOptions(opts...)
	return extension.GetConfigCenter(options.Name, options.Internal)
}
