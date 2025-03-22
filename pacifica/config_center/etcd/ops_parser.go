package etcd

import (
	"strings"

	"github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
	"github.com/yeyeye2333/PacificaMQ/pkg/conv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type parser struct {
	events                                       []*common.WatchEvent
	nextevent                                    *common.WatchEvent
	leaderPrefix, versionPrefix, followersPrefix string
}

func (p *parser) init(leaderPrefix, versionPrefix, followersPrefix string) {
	p.leaderPrefix = leaderPrefix
	p.versionPrefix = versionPrefix
	p.followersPrefix = followersPrefix
	p.events = make([]*common.WatchEvent, 0)
	p.newEvent()
}

func (p *parser) newEvent() {
	p.nextevent = &common.WatchEvent{}
	p.nextevent.Init()
}

// 操作执行或解析都得将version放在最后
func (p *parser) parse(response *clientv3.Event) {
	k := string(response.Kv.Key)
	v := string(response.Kv.Value)
	switch {
	case (response.Type == clientv3.EventTypePut) && (strings.HasPrefix(k, p.leaderPrefix)):
		p.nextevent.Leader = v
		p.nextevent.Type = common.EventReplaceLeader
	case (response.Type == clientv3.EventTypePut) && (strings.HasPrefix(k, p.followersPrefix)):
		p.nextevent.Followers[strings.Clone(k[len(p.followersPrefix):])] = struct{}{}
		p.nextevent.Type = common.EventAddFollower
	case (response.Type == clientv3.EventTypeDelete) && (strings.HasPrefix(k, p.followersPrefix)):
		p.nextevent.Followers[strings.Clone(k[len(p.followersPrefix):])] = struct{}{}
		if p.nextevent.Leader == "" {
			p.nextevent.Type = common.EventRemoveFollower
		}
	case (response.Type == clientv3.EventTypePut) && (strings.HasPrefix(k, p.versionPrefix)):
		p.nextevent.Version = conv.BytesToUint64(response.Kv.Value)
		p.events = append(p.events, p.nextevent)
		p.newEvent()
	}
}

func (p *parser) getEvents() []*common.WatchEvent {
	ret := p.events
	p.events = make([]*common.WatchEvent, 0)
	return ret
}
