package zap

import (
	"os"
	"strings"

	"github.com/yeyeye2333/PacificaMQ/internal/extension"
	"github.com/yeyeye2333/PacificaMQ/internal/logger/common"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	extension.SetLogger("zap", NewLogger)
}

type zap_logger struct {
	*zap.SugaredLogger
	lv zap.AtomicLevel
}

func (l *zap_logger) SetLevel(level string) bool {
	var lv zapcore.Level
	if err := lv.Set(level); err != nil {
		return false
	}
	l.lv.SetLevel(lv)
	return true
}

var NewLogger extension.LoggerFactory = func(opts common.InternalOptions) (logger common.Logger, err error) {
	var (
		level    string
		lv       zapcore.Level
		sync     []zapcore.WriteSyncer
		encoder  zapcore.Encoder
		appender []string
	)

	level = opts.Level
	if err = lv.UnmarshalText([]byte(level)); err != nil {
		return nil, err
	}

	appender = opts.Appender
	for _, apt := range appender {
		switch apt {
		case "console":
			sync = append(sync, zapcore.AddSync(os.Stdout))
		default:
			file, err := os.OpenFile(apt, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				return nil, err
			}
			sync = append(sync, zapcore.AddSync(file))
		}
	}

	format := opts.Format
	switch strings.ToLower(format) {
	case "text":
		encoder = zapcore.NewConsoleEncoder(encoderConfig())
	case "json":
		ec := encoderConfig()
		ec.EncodeLevel = zapcore.CapitalLevelEncoder
		encoder = zapcore.NewJSONEncoder(ec)
	default:
		encoder = zapcore.NewConsoleEncoder(encoderConfig())
	}

	var zap_logger zap_logger
	zap_logger.lv = zap.NewAtomicLevelAt(lv)
	zap_logger.SugaredLogger = zap.New(zapcore.NewCore(encoder, zapcore.NewMultiWriteSyncer(sync...), zap_logger.lv),
		zap.AddCaller(), zap.AddCallerSkip(0)).Sugar()
	logger = &zap_logger
	return logger, nil
}

// 默认用于控制台输出的配置
func encoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		TimeKey:        "time",
		CallerKey:      "line",
		NameKey:        "logger",
		StacktraceKey:  "stacktrace",
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05"),
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}
