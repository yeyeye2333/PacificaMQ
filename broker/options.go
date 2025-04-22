package broker

import (
	regCM "github.com/yeyeye2333/PacificaMQ/internal/registry/common"
	"github.com/yeyeye2333/PacificaMQ/internal/storage"
	"github.com/yeyeye2333/PacificaMQ/pacifica"
	"github.com/yeyeye2333/PacificaMQ/pkg/defaults"
)

type partitionOptions struct {
	StorageOpts  *storage.Options  `default:"-"`
	PacificaOpts *pacifica.Options `default:"-"`
	PartitionID  int32             `default:"0"`
	MaxTimeOut   int64             `default:"2000"` //单位：ms
}

// 主要用于defaultCf
type Options struct {
	PartitionOpts *partitionOptions
	RegistryOpts  *regCM.Options `default:"-"`
}

func defaultOptions() *Options {
	options := &Options{}
	defaults.MustSet(options)

	options.PartitionOpts.StorageOpts = storage.NewOptions()
	options.PartitionOpts.PacificaOpts = pacifica.NewOptions()
	options.RegistryOpts = regCM.NewOptions()
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

// partitionOpts
func WithStorage(opts ...storage.Option) Option {
	return func(o *Options) {
		o.PartitionOpts.StorageOpts = storage.NewOptions(opts...)
	}
}

func WithPacifica(opts ...pacifica.Option) Option {
	return func(o *Options) {
		o.PartitionOpts.PacificaOpts = pacifica.NewOptions(opts...)
	}
}

func WithPartitionID(id int32) Option {
	return func(o *Options) {
		o.PartitionOpts.PartitionID = id
	}
}

func WithMaxTimeOut(t int64) Option {
	return func(o *Options) {
		o.PartitionOpts.MaxTimeOut = t
	}
}

// other
func WithRegistry(opts ...regCM.Option) Option {
	return func(o *Options) {
		o.RegistryOpts = regCM.NewOptions(opts...)
	}
}
