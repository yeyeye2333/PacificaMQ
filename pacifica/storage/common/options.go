package common

import "github.com/yeyeye2333/PacificaMQ/pkg/defaults"

type InternalOptions struct {
}

type Options struct {
	Name     string
	Internal InternalOptions
}

func defaultOptions() *Options {
	opts := &Options{}
	defaults.MustSet(opts)
	return opts
}

func NewOptions(opts ...Option) *Options {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}

type Option func(*Options)

func WithStorage(name string) Option {
	return func(opts *Options) {
		opts.Name = name
	}
}
