package storage

import (
	"github.com/linxGnu/grocksdb"
	"github.com/yeyeye2333/PacificaMQ/pkg/defaults"
)

type Options struct {
	Path string `default:"-"`

	Options        *grocksdb.Options                `default:"-"`
	TableOptions   *grocksdb.BlockBasedTableOptions `default:"-"`
	BlockCacheSize uint64                           `default:"10000000"` // 10MB左右

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
	options.ReadOptions = grocksdb.NewDefaultReadOptions()
	options.WriteOptions = grocksdb.NewDefaultWriteOptions()

	options.Options.SetCreateIfMissing(true)
	options.Options.SetCreateIfMissingColumnFamilies(true)

	opts := []Option{WithSync(false),
		WithNumLevels(2),
		WithWriteBufferSize(32 << 20),
		WithMaxWriteBufferNumber(4),
		WithMinWriteBufferNumberToMerge(1),
		WithMaxBackgroundJobs(2),
		WithMemManagerSize(300 << 20),
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

func NewOptions(opts ...Option) *Options {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}

type Option func(*Options)

// Path 相关配置
func WithPath(path string) Option {
	return func(o *Options) {
		o.Path = path
	}
}

// WriteOptions 相关配置
func WithSync(isSync bool) Option {
	return func(o *Options) {
		o.WriteOptions.SetSync(isSync)
	}
}

// Options 相关配置
// level层数
func WithNumLevels(numLevels int) Option {
	return func(o *Options) {
		o.Options.SetNumLevels(numLevels)
	}
}

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
