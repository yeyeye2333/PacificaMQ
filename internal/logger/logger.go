package logger

import (
	"github.com/yeyeye2333/PacificaMQ/internal/extension"
	"github.com/yeyeye2333/PacificaMQ/internal/logger/common"
	_ "github.com/yeyeye2333/PacificaMQ/internal/logger/zap"
)

func init() {
	//默认使用zap
	default_logger, err := NewLogger()
	if err != nil {
		panic(err)
	}
	SetLogger(default_logger)
}

var logger common.Logger

func NewLogger(opts ...common.Option) (common.Logger, error) {
	options := common.NewOptions(opts...)
	return extension.GetLogger(options.Driver, options.Internal)
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
