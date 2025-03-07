package logger

import (
	"github.com/yeyeye2333/PacificaMQ/extension"
	"github.com/yeyeye2333/PacificaMQ/logger/common"
	_ "github.com/yeyeye2333/PacificaMQ/logger/zap"
)

func init() {
	//默认使用zap
	default_logger, err := NewLogger(common.NewOptions())
	if err != nil {
		panic(err)
	}
	SetLogger(default_logger)
}

var logger common.Logger

func NewLogger(opts *common.Options) (common.Logger, error) {
	return extension.GetLogger(opts.Driver, opts.Internal)
}

func SetLogger(log common.Logger) {
	logger = log
}

func GetLogger() common.Logger {
	return logger
}

func SetLevel(level string) bool {
	if sl, ok := logger.(common.SetLevel_Logger); ok {
		return sl.SetLevel(level)
	}
	return false
}
