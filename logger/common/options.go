package common

import (
	"github.com/yeyeye2333/PacificaMQ/pkg/defaults"
)

type InternalOptions struct {
	Level    string   `default:"info"`
	Format   string   `default:"text"`
	Appender []string `default:"[\"console\"]"`
}

type Options struct {
	Driver   string `default:"zap"`
	Internal InternalOptions
}

func defaultOptions() *Options {
	opts := &Options{}
	defaults.MustSet(opts)
	return opts
}

func NewOptions(opts ...Option) *Options {
	Options := defaultOptions()
	for _, opt := range opts {
		opt(Options)
	}
	return Options
}

type Option func(*Options)

// Option for Options.
func WithDriver(driver string) Option {
	return func(o *Options) {
		o.Driver = driver
	}
}

// Option for InternalOptions.
func WithLevel(level string) Option {
	return func(o *Options) {
		o.Internal.Level = level
	}
}

func WithFormat(format string) Option {
	return func(o *Options) {
		o.Internal.Format = format
	}
}

func WithAppender(appender ...string) Option {
	return func(o *Options) {
		o.Internal.Appender = append(o.Internal.Appender, appender...)
	}
}
