package pacifica

import (
	"github.com/yeyeye2333/PacificaMQ/internal/logger"
	logCM "github.com/yeyeye2333/PacificaMQ/internal/logger/common"
	ccCM "github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
	"github.com/yeyeye2333/PacificaMQ/pacifica/storage"
	"github.com/yeyeye2333/PacificaMQ/pkg/defaults"
)

type Options struct {
	CcOpts      *ccCM.Options    `default:"-"`
	StorageOpts *storage.Options `default:"-"`

	Address  string       `default:"127.0.0.1:50001"`
	Snapshot Snapshoter   `default:"-"`
	Logger   logCM.Logger `default:"-"`
	//一次给从节点发送的最大消息数
	MaxNumsOnce uint64 `default:"100"`
	// 单位:ms
	FollowerPeriod int64 `default:"2000"`
	LearnerPeriod  int64 `default:"2000"`
	LeaderPeriod   int64 `default:"1000"`

	// 回调
	OnBecomeLeader func(version uint64, leader string, followers []string) error `default:"-"`
}

func defaultOptions() *Options {
	options := &Options{}
	defaults.MustSet(options)

	options.CcOpts = ccCM.NewOptions()
	options.StorageOpts = storage.NewOptions()
	opts := []Option{
		WithLogger(logger.GetLogger()),
		WithOnBecomeLeader(func(version uint64, leader string, followers []string) error { return nil }),
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

func WithConfigCenter(opts ...ccCM.Option) Option {
	return func(options *Options) {
		options.CcOpts = ccCM.NewOptions(opts...)
	}
}

func WithStorage(opts ...storage.Option) Option {
	return func(options *Options) {
		options.StorageOpts = storage.NewOptions(opts...)
	}
}

func WithAddress(address string) Option {
	return func(options *Options) {
		options.Address = address
	}
}

func WithSnapshot(snapshot Snapshoter) Option {
	return func(options *Options) {
		options.Snapshot = snapshot
	}
}

func WithLogger(logger logCM.Logger) Option {
	return func(options *Options) {
		options.Logger = logger
	}
}

func WithFollowerPeriod(period int64) Option {
	return func(options *Options) {
		options.FollowerPeriod = period
	}
}

func WithLearnerPeriod(period int64) Option {
	return func(options *Options) {
		options.LearnerPeriod = period
	}
}

func WithLeaderPeriod(period int64) Option {
	return func(options *Options) {
		options.LeaderPeriod = period
	}
}

func WithMaxNumsOnce(max uint64) Option {
	return func(options *Options) {
		options.MaxNumsOnce = max
	}
}

func WithOnBecomeLeader(f func(version uint64, leader string, followers []string) error) Option {
	return func(options *Options) {
		options.OnBecomeLeader = f
	}
}
