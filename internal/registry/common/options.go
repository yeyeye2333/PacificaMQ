package common

import (
	"github.com/yeyeye2333/PacificaMQ/pkg/defaults"
)

type InternalOptions struct {
	NameSpace string   `default:"/PacificaMQ"`
	Cluster   string   `default:"-"`
	Address   []string `default:"[\"127.0.0.1:2379\"]"`
	TTL       int64    `default:"10"`
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

func WithRegistry(name string) Option {
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

func WithCluster(cluster string) Option {
	return func(opts *Options) {
		opts.Internal.Cluster = cluster
	}
}

func WithAddress(address []string) Option {
	return func(opts *Options) {
		opts.Internal.Address = address
	}
}

func WithTTL(ttl int64) Option {
	return func(opts *Options) {
		opts.Internal.TTL = ttl
	}
}
