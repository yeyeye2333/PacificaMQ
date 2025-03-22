package pacifica

import (
	ccCM "github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
	"github.com/yeyeye2333/PacificaMQ/pkg/defaults"
)

type Options struct {
	ccOpts []ccCM.Option
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
