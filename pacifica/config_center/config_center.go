package config_center

import (
	"github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
	_ "github.com/yeyeye2333/PacificaMQ/pacifica/config_center/etcd"
	"github.com/yeyeye2333/PacificaMQ/pacifica/extension"
)

func NewConfigCenter(options *common.Options) (common.ConfigCenter, error) {
	return extension.GetConfigCenter(options.Name, options.Internal)
}
