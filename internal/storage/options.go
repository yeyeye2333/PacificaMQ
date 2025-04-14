package storage

import (
	"github.com/linxGnu/grocksdb"
	"github.com/yeyeye2333/PacificaMQ/pkg/defaults"
)

// 主要用于defaultCf
type Options struct {
	Path string `default:"-"`

	Options        *grocksdb.Options                `default:"-"`
	TableOptions   *grocksdb.BlockBasedTableOptions `default:"-"`
	BlockCacheSize uint64                           `default:"100000000"` // 100MB左右

	FifoOPtions       *grocksdb.FIFOCompactionOptions `default:"-"`
	MaxTableFilesSize uint64                          `default:"1000000000"` // 1GB左右

	ReadOptions  *grocksdb.ReadOptions  `default:"-"`
	WriteOptions *grocksdb.WriteOptions `default:"-"`

	minBytesPerBatch uint64 `default:"10000"` // 10kb左右
	maxWaitPerBatch  uint32 `default:"5"`     // 5ms
}

func defaultOptions() *Options {
	options := &Options{}
	defaults.MustSet(options)

	options.Options = grocksdb.NewDefaultOptions()
	options.TableOptions = grocksdb.NewDefaultBlockBasedTableOptions()
	options.FifoOPtions = grocksdb.NewDefaultFIFOCompactionOptions()
	options.ReadOptions = grocksdb.NewDefaultReadOptions()
	options.WriteOptions = grocksdb.NewDefaultWriteOptions()

	options.Options.SetCreateIfMissing(true)
	options.Options.SetCreateIfMissingColumnFamilies(true)
	options.Options.SetCompactionStyle(2)
	opts := []Option{WithDisableWAL(true),
		WithWriteBufferSize(64 << 20),
		WithMaxWriteBufferNumber(8),
		WithMinWriteBufferNumberToMerge(1),
		WithMaxBackgroundJobs(4),
		WithEnabledPipelinedWrite(false),
		WithMemManagerSize(300 << 20),
	}
	for _, opt := range opts {
		opt(options)
	}

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

// Path 相关配置
func WithPath(path string) Option {
	return func(o *Options) {
		o.Path = path
	}
}

// FifoCompaction 相关配置
func WithMaxTableFilesSize(size uint64) Option {
	return func(o *Options) {
		o.FifoOPtions.SetMaxTableFilesSize(size)
	}
}

// WriteOptions 相关配置
func WithDisableWAL(enabled bool) Option {
	return func(o *Options) {
		o.WriteOptions.DisableWAL(enabled)
	}
}

// Options 相关配置
// memTable/L0层 相关
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

// 后台flush+compaction线程数
func WithMaxBackgroundJobs(number int) Option {
	return func(o *Options) {
		o.Options.SetMaxBackgroundJobs(number)
	}
}

// 流水线
func WithEnabledPipelinedWrite(enabled bool) Option {
	return func(o *Options) {
		o.Options.SetEnablePipelinedWrite(enabled)
	}
}

// rocksdb总内存的软限制
func WithMemManagerSize(bufferSize int) Option {
	return func(o *Options) {
		wbm := grocksdb.NewWriteBufferManager(bufferSize, false)
		o.Options.SetWriteBufferManager(wbm)
	}
}

// 共享块缓存
func WithBlockCacheSize(size uint64) Option {
	return func(o *Options) {
		o.BlockCacheSize = size
	}
}

// 每次批量写入的最大字节数
func WithMinBytesPerBatch(size uint64) Option {
	return func(o *Options) {
		o.minBytesPerBatch = size
	}
}

// 每次批量写入的最大等待时间
func WithMaxWaitPerBatch(wait uint32) Option {
	return func(o *Options) {
		o.maxWaitPerBatch = wait
	}
}
