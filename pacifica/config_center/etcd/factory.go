package etcd

import (
	"github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
	"github.com/yeyeye2333/PacificaMQ/pacifica/extension"
)

func init() {
	extension.SetConfigCenter("etcd", NewConfigCenter)
}

type EtcdFactory struct{}

func (*EtcdFactory) NewConfigCenter(opts common.InternalOptions) (common.ConfigCenter, error) {
	configCenter := &etcdConfigCenter{InternalOptions: opts}

	return configCenter, nil
}
