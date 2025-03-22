package common

import (
	"github.com/yeyeye2333/PacificaMQ/pkg/defaults"
)

type InternalOptions struct {
	NameSpace string   `default:"/Pacifica"`
	Group     string   `default:"-"`
	Address   []string `default:"[\"127.0.0.1:2379\"]"`
}

type Options struct {
	Name     string `default:"etcd"`
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

func WithConfigCenter(name string) Option {
	return func(opts *Options) {
		opts.Name = name
	}
}

// Option for InternalOptions.
func WithNameSpace(namespace string) Option {
	return func(opts *Options) {
		opts.Internal.NameSpace = namespace
	}
}

func WithGroup(group string) Option {
	return func(opts *Options) {
		opts.Internal.Group = group
	}
}

func WithAddress(address []string) Option {
	return func(opts *Options) {
		opts.Internal.Address = address
	}
}
