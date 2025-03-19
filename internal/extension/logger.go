package extension

import (
	"fmt"

	logger "github.com/yeyeye2333/PacificaMQ/internal/logger/common"
)

type LoggerFactory = func(logger.InternalOptions) (logger.Logger, error)

var logs = make(map[string]LoggerFactory)

func SetLogger(driver string, lf LoggerFactory) {
	logs[driver] = lf
}

func GetLogger(driver string, options logger.InternalOptions) (logger.Logger, error) {
	if lf, ok := logs[driver]; ok {
		return lf(options)
	} else {
		return nil, fmt.Errorf("logger for %s does not exist", driver)
	}
}
