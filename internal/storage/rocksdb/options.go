package rocksdb

import (
	"github.com/linxGnu/grocksdb"
	"github.com/yeyeye2333/PacificaMQ/pkg/defaults"
)

type Options struct {
	Options          *grocksdb.Options                `default:"-"`
	TableOptions     *grocksdb.BlockBasedTableOptions `default:"-"`
	FifoOPtions      *grocksdb.FIFOCompactionOptions  `default:"-"`
	Block_cache_size uint64                           `default:"100000000"` // 100MB左右
}

func defaultOptions() *Options {
	opts := &Options{}
	defaults.MustSet(opts)

	opts.Options = grocksdb.NewDefaultOptions()
	opts.TableOptions = grocksdb.NewDefaultBlockBasedTableOptions()
	opts.FifoOPtions = grocksdb.NewDefaultFIFOCompactionOptions()

	opts.Options.SetCreateIfMissing(true)
	opts.Options.SetCreateIfMissingColumnFamilies(true)
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

// FifoCompaction 相关配置
func WithMaxTableFilesSize(size uint64) Option {
	return func(o *Options) {
		o.FifoOPtions.SetMaxTableFilesSize(size)
	}
}

// 常用配置
func WithWriteBufferSize(size uint64) Option {
	return func(o *Options) {
		o.Options.SetWriteBufferSize(size)
	}
}

func WithMaxWriteBufferNumber(number int) Option {
	return func(o *Options) {
		o.Options.SetMaxWriteBufferNumber(number)
	}
}

func WithMinWriteBufferNumberToMerge(number int) Option {
	return func(o *Options) {
		o.Options.SetMinWriteBufferNumberToMerge(number)
	}
}

func WithMaxBackgroundJobs(number int) Option {
	return func(o *Options) {
		o.Options.SetMaxBackgroundJobs(number)
	}
}

func WithEnabledPipelinedWrite(enabled bool) Option {
	return func(o *Options) {
		o.Options.SetEnablePipelinedWrite(enabled)
	}
}
