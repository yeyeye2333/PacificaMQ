package server

import (
	regCM "github.com/yeyeye2333/PacificaMQ/internal/registry/common"
	"github.com/yeyeye2333/PacificaMQ/internal/storage"
	"github.com/yeyeye2333/PacificaMQ/pkg/defaults"
)

// 主要用于defaultCf
type Options struct {
	storageOpt  []storage.Option `default:"-"`
	registryOpt []regCM.Option
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
