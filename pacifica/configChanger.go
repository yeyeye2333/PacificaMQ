package pacifica

import (
	"sync"

	logCM "github.com/yeyeye2333/PacificaMQ/internal/logger/common"
	"github.com/yeyeye2333/PacificaMQ/pacifica/common"
	"github.com/yeyeye2333/PacificaMQ/pacifica/config_center"
	ccCM "github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
)

type configChanger struct {
	configCenter ccCM.ConfigCenter
	me           common.NodeID
	logCM.Logger
	// 回调前更新完config
	becomeLeader   func()
	becomeFollower func()
	becomeLearner  func()

	// 保护config外层Leader和Version
	mu            sync.RWMutex
	config        common.Config
	latestVersion common.Version
}

func (c *configChanger) Process(event *ccCM.WatchEvent) {
	c.Info("process event", event)
	c.config.ClusterConfig.Version = event.Version
	switch event.Type {
	case ccCM.EventAddFollower:
		for follower := range event.Followers {
			c.config.ClusterConfig.Followers[follower] = struct{}{}
			if c.becomeFollower != nil && follower == c.me {
				func() {
					c.mu.Lock()
					if c.latestVersion < c.config.ClusterConfig.Version {
						c.latestVersion = c.config.ClusterConfig.Version
						c.mu.Unlock()
						c.becomeFollower()
					} else {
						c.mu.Unlock()
					}
				}()
			}
		}
	case ccCM.EventRemoveFollower:
		for follower := range event.Followers {
			delete(c.config.ClusterConfig.Followers, follower)
			if c.becomeLearner != nil && follower == c.me {
				func() {
					c.mu.Lock()
					if c.latestVersion < c.config.ClusterConfig.Version {
						c.latestVersion = c.config.ClusterConfig.Version
						c.mu.Unlock()
						c.becomeLearner()
					} else {
						c.mu.Unlock()
					}
				}()
			}
		}
	case ccCM.EventReplaceLeader:
		c.config.ClusterConfig.Leader = event.Leader
		c.ChangeLeader(c.config.ClusterConfig.Leader, c.config.ClusterConfig.Version)
	}
}

func (c *configChanger) Start(me common.NodeID, logger logCM.Logger, opts *ccCM.Options) error {
	c.Logger = logger
	c.me = me
	c.config.Init()
	configCenter, err := config_center.NewConfigCenter(opts)
	if err != nil {
		return err
	}
	err = configCenter.Start()
	if err != nil {
		return err
	}
	c.configCenter = configCenter

	c.configCenter.WatchConfig(c)
	config, err := c.configCenter.GetConfig()
	c.Info("get config from config center", config)
	if err != nil {
		return err
	}
	if config.Version > 0 {
		c.ChangeLeader(config.Leader, config.Version)
	} else {
		c.ChangeLeader(c.me, 1)
	}
	c.config.ClusterConfig = *config
	return nil
}

func (c *configChanger) Close() error {
	return c.configCenter.Close()
}

func (c *configChanger) ChangeLeader(newLeader common.NodeID, newVersion common.Version) {
	c.mu.Lock()
	if c.config.Version < newVersion {
		c.Info("change leader to ", newLeader, " version ", newVersion)
		c.latestVersion = newVersion
		// 回调前更新config
		oldLeader := c.config.Leader
		c.config.Leader = newLeader
		c.config.Version = newVersion
		c.mu.Unlock()
		if c.becomeLearner != nil && oldLeader == c.me {
			c.becomeLearner()
		} else if c.becomeLeader != nil && c.config.Leader == c.me {
			c.becomeLeader()
		}
	} else {
		c.mu.Unlock()
	}
}

func (c *configChanger) GetLeader() (common.NodeID, common.Version) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config.Leader, c.config.Version
}

func (c *configChanger) GetFollowers() []common.NodeID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	followers := make([]common.NodeID, 0, len(c.config.ClusterConfig.Followers))
	for follower := range c.config.ClusterConfig.Followers {
		followers = append(followers, follower)
	}
	return followers
}

func (c *configChanger) SetBecomeLeader(cb func()) {
	c.becomeLeader = cb
}

func (c *configChanger) SetBecomeFollower(cb func()) {
	c.becomeFollower = cb
}

func (c *configChanger) SetBecomeLearner(cb func()) {
	c.becomeLearner = cb
}
