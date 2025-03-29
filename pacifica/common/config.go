package common

import (
	ccCommon "github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
)

type Config struct {
	ccCommon.ClusterConfig
	Leader  NodeID
	Version Version
}
