package extension

import (
	"fmt"

	logger "github.com/yeyeye2333/PacificaMQ/logger/common"
)

type LoggerFactory = func(logger.InternalOptions) (logger.Logger, error)

var logs = make(map[string]LoggerFactory)

func SetLogger(driver string, log LoggerFactory) {
	logs[driver] = log
}

func GetLogger(driver string, options logger.InternalOptions) (logger.Logger, error) {

	if logs[driver] != nil {
		return logs[driver](options)
	} else {
		return nil, fmt.Errorf("logger for %s does not exist.", driver)
	}
}
