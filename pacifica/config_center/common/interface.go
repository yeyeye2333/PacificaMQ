package common

import (
	"errors"
)

type ConfigCenter interface {
	Start() error
	Close() error

	// 获取配置
	GetConfig() (*ClusterConfig, error)
	WatchConfig(configWatcher ConfigWatcher)

	// 原子更新配置
	ReplaceLeader(new_leader string, new_version uint64) error
	AddFollower(follower string, new_version uint64) error
	RemoveFollower(follower string, new_version uint64) error
}

var (
	ErrVersionBehind = errors.New("config version behind")
)
