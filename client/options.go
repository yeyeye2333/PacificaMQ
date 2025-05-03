package client

import (
	"github.com/yeyeye2333/PacificaMQ/pkg/defaults"
)

type Options struct {
	Address     string      `default:"127.0.0.1:50001"`
	EtcdAddress []string    `default:"127.0.0.1:2379"`
	MaxTimeOut  int64       `default:"3000"` //单位：ms
	Interceptor Interceptor // 消息拦截器
	Partitioner Partitioner // 分区函数
}

func defaultOptions() *Options {
	options := &Options{}
	defaults.MustSet(options)
	return options
}

func NewOptions(opts ...Option) *Options {
	Options := defaultOptions()
	for _, opt := range opts {
		opt(Options)
	}
	return Options
}

type Option func(*Options)

func WithAddress(addr string) Option {
	return func(o *Options) {
		o.Address = addr
	}
}

func WithEtcdAddress(addrs []string) Option {
	return func(o *Options) {
		o.EtcdAddress = addrs
	}
}

func WithMaxTimeOut(timeout int64) Option {
	return func(o *Options) {
		o.MaxTimeOut = timeout
	}
}

func WithInterceptor(interceptor Interceptor) Option {
	return func(o *Options) {
		o.Interceptor = interceptor
	}
}

func WithPartitioner(partitioner Partitioner) Option {
	return func(o *Options) {
		o.Partitioner = partitioner
	}
}
