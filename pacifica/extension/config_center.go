package extension

import (
	"fmt"

	config_center "github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
)

type ConfigCenterFactory = func(config_center.InternalOptions) (config_center.ConfigCenter, error)

var configCenters = make(map[string]ConfigCenterFactory)

func SetConfigCenter(name string, ccf ConfigCenterFactory) {
	configCenters[name] = ccf
}

func GetConfigCenter(name string, options config_center.InternalOptions) (config_center.ConfigCenter, error) {
	if ccf, ok := configCenters[name]; ok {
		return ccf(options)
	}
	return nil, fmt.Errorf("config center %s not found", name)
}
