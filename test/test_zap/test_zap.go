package main

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	logger1, _ := zap.NewDevelopment()
	defer logger1.Sync()
	logger1.Info("test_logger1")
	logger1.Warn("test_logger1 Stacktrace")

	logger2 := zap.New(
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
			zapcore.Lock((os.Stdout)),
			zap.LevelEnablerFunc(func(lv zapcore.Level) bool {
				return lv == zapcore.InfoLevel || lv == zapcore.ErrorLevel
			})),
		zap.AddCaller(), zap.AddCallerSkip(0))
	defer logger2.Sync()
	logger2.Info("test_logger2 Info")
	logger2.Warn("tset_logger2 Warn")
}
