package pacifica

import (
	"sync"

	"github.com/yeyeye2333/PacificaMQ/pacifica/common"
	"github.com/yeyeye2333/PacificaMQ/pacifica/config_center"
	ccCM "github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
)

type configChanger struct {
	configCenter ccCM.ConfigCenter

	config common.Config
	// 保护config外层Leader和Version
	mu sync.RWMutex
}

func (c *configChanger) Process(event *ccCM.WatchEvent) {
	c.config.ClusterConfig.Version = event.Version
	switch event.Type {
	case ccCM.EventAddFollower:
		for follower := range event.Followers {
			c.config.ClusterConfig.Followers[follower] = struct{}{}
		}
	case ccCM.EventRemoveFollower:
		for follower := range event.Followers {
			delete(c.config.ClusterConfig.Followers, follower)
		}
	case ccCM.EventReplaceLeader:
		c.config.ClusterConfig.Leader = event.Leader
		c.ChangeLeader(c.config.ClusterConfig.Leader, c.config.ClusterConfig.Version)
	}
}

func (c *configChanger) Start(opts ...ccCM.Option) error {
	c.config.Init()
	configCenter, err := config_center.NewConfigCenter(opts...)
	if err != nil {
		return err
	}
	c.configCenter = configCenter

	c.configCenter.WatchConfig(c)
	config, err := c.configCenter.GetConfig()
	if err != nil {
		return err
	}
	c.ChangeLeader(config.Leader, config.Version)
	c.config.ClusterConfig = *config
	return nil
}

func (c *configChanger) ChangeLeader(newLeader common.NodeID, newVersion common.Verson) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.config.Version < newVersion {
		c.config.Leader = newLeader
		c.config.Version = newVersion
	}
}

func (c *configChanger) GetLeader() (common.NodeID, common.Verson) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config.Leader, c.config.Version
}
