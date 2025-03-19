package etcd

import (
	"context"
	"strings"

	"github.com/yeyeye2333/PacificaMQ/internal/logger"
	"github.com/yeyeye2333/PacificaMQ/pacifica/config_center/common"
	"github.com/yeyeye2333/PacificaMQ/pacifica/extension"
	"github.com/yeyeye2333/PacificaMQ/pkg/conv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func init() {
	extension.SetConfigCenter("etcd", NewConfigCenter)
}

const (
	Sep = "/"
)

var NewConfigCenter extension.ConfigCenterFactory = func(opts common.InternalOptions) (common.ConfigCenter, error) {
	configCenter := &etcdConfigCenter{InternalOptions: opts}

	return configCenter, nil
}

type etcdConfigCenter struct {
	common.InternalOptions
	rootPath        string
	leaderPrefix    string
	followersPrefix string
	versionPrefix   string

	ctx    context.Context
	cancel context.CancelFunc

	cli     *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

// 接口实现
func (cc *etcdConfigCenter) Start() error {
	var rootPath string
	if cc.Group != "" {
		rootPath = cc.NameSpace + Sep + cc.Group + Sep
	} else {
		rootPath = cc.NameSpace + Sep
	}
	cc.rootPath = rootPath
	cc.leaderPrefix = cc.rootPath + "Leader"
	cc.followersPrefix = cc.rootPath + "Followers"
	cc.versionPrefix = cc.rootPath + "Version"
	logger.Debugf("etcd config center start with root path: %s ,leader prefix: %s, followers prefix: %s, version prefix: %s",
		cc.rootPath, cc.leaderPrefix, cc.followersPrefix, cc.versionPrefix)

	ctx, cancel := context.WithCancel(context.Background())
	cc.ctx = ctx
	cc.cancel = cancel

	cli, err := clientv3.New(clientv3.Config{
		Endpoints: cc.Address,
	})
	if err != nil {
		return err
	}
	cc.cli = cli
	cc.kv = cc.cli.KV
	cc.lease = cc.cli.Lease
	cc.watcher = cc.cli.Watcher
	return nil
}

func (cc *etcdConfigCenter) Close() error {
	cc.cancel()
	return cc.cli.Close()
}

func (cc *etcdConfigCenter) GetConfig() (*common.ClusterConfig, error) {
	response, err := cc.kv.Get(cc.ctx, cc.rootPath, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	clusterConfig := &common.ClusterConfig{Followers: make(map[string]struct{})}

	//  至少存在leader/follower和version
	if len(response.Kvs) < 2 {
		clusterConfig.Version = 0
		return clusterConfig, nil
	}

	for _, kv := range response.Kvs {
		k := string(kv.Key)
		v := string(kv.Value)
		switch {
		case strings.HasPrefix(k, cc.leaderPrefix):
			clusterConfig.Leader = v
		case strings.HasPrefix(k, cc.followersPrefix):
			clusterConfig.Followers[strings.Clone(k[len(cc.followersPrefix):])] = struct{}{}
		case strings.HasPrefix(k, cc.versionPrefix):
			version := conv.BytesToInt32(kv.Value)
			clusterConfig.Version = int(version)
		}
	}
	return clusterConfig, nil
}

func (cc *etcdConfigCenter) WatchConfig(configWatcher common.ConfigWatcher) {
	watchCh := cc.watcher.Watch(cc.ctx, cc.rootPath)
	go func() {
		for response := range watchCh {
			for _, event := range response.Events {
				k := string(event.Kv.Key)
				v := string(event.Kv.Value)
				parm := common.WatchEvent{}
				switch {
				case (event.Type == clientv3.EventTypePut) && (strings.HasPrefix(k, cc.leaderPrefix)):
					parm.Event = common.EventUpdateLeader
					parm.Value = v
				case (event.Type == clientv3.EventTypePut) && (strings.HasPrefix(k, cc.versionPrefix)):
					parm.Event = common.EventUpdateVersion
					parm.Value = conv.BytesToInt32(event.Kv.Value)
				case (event.Type == clientv3.EventTypePut) && (strings.HasPrefix(k, cc.followersPrefix)):
					parm.Event = common.EventAddFollower
					parm.Value = strings.Clone(k[len(cc.followersPrefix):])
				case (event.Type == clientv3.EventTypeDelete) && (strings.HasPrefix(k, cc.followersPrefix)):
					parm.Event = common.EventDelFollower
					parm.Value = strings.Clone(k[len(cc.followersPrefix):])
				default:
					continue
				}
				configWatcher.Process(parm)
			}
		}
	}()
}

func (cc *etcdConfigCenter) ReplaceLeader(new_leader string, new_version int) error {
	responses, err := cc.kv.Txn(cc.ctx).If(
		clientv3.Compare(clientv3.CreateRevision(cc.versionPrefix), "!=", 0),
	).Then(
		clientv3.OpTxn(
			[]clientv3.Cmp{
				clientv3.Compare(clientv3.Value(cc.versionPrefix), "<", string(conv.Int32ToBytes(int32(new_version)))),
			},
			[]clientv3.Op{
				clientv3.OpPut(cc.leaderPrefix, new_leader),
				clientv3.OpPut(cc.versionPrefix, string(conv.Int32ToBytes(int32(new_version)))),
				clientv3.OpDelete(cc.followersPrefix + Sep + new_leader),
			},
			[]clientv3.Op{},
		),
	).Else(
		clientv3.OpPut(cc.leaderPrefix, new_leader),
		clientv3.OpPut(cc.versionPrefix, string(conv.Int32ToBytes(int32(new_version)))),
	).Commit()

	if err != nil {
		return err
	}
	if responses.Succeeded && !responses.Responses[0].GetResponseTxn().Succeeded {
		return common.ErrVersionBehind
	}
	return nil
}

func (cc *etcdConfigCenter) AddFollower(follower string, new_version int) error {
	responses, err := cc.kv.Txn(cc.ctx).If(
		clientv3.Compare(clientv3.CreateRevision(cc.versionPrefix), "!=", 0),
	).Then(
		clientv3.OpTxn(
			[]clientv3.Cmp{
				clientv3.Compare(clientv3.Value(cc.versionPrefix), "<", string(conv.Int32ToBytes(int32(new_version)))),
			},
			[]clientv3.Op{
				clientv3.OpPut(cc.followersPrefix+Sep+follower, ""),
				clientv3.OpPut(cc.versionPrefix, string(conv.Int32ToBytes(int32(new_version)))),
			},
			[]clientv3.Op{},
		),
	).Else(
		clientv3.OpPut(cc.followersPrefix+Sep+follower, ""),
		clientv3.OpPut(cc.versionPrefix, string(conv.Int32ToBytes(int32(new_version)))),
	).Commit()

	if err != nil {
		return err
	}
	if !responses.Succeeded {
		return common.ErrVersionBehind
	}
	return nil
}

func (cc *etcdConfigCenter) RemoveFollower(follower string, new_version int) error {
	responses, err := cc.kv.Txn(cc.ctx).If(
		clientv3.Compare(clientv3.CreateRevision(cc.versionPrefix), "!=", 0),
	).Then(
		clientv3.OpTxn(
			[]clientv3.Cmp{
				clientv3.Compare(clientv3.Value(cc.versionPrefix), "<", string(conv.Int32ToBytes(int32(new_version)))),
			},
			[]clientv3.Op{
				clientv3.OpDelete(cc.followersPrefix + Sep + follower),
				clientv3.OpPut(cc.versionPrefix, string(conv.Int32ToBytes(int32(new_version)))),
			},
			[]clientv3.Op{},
		),
	).Else(
		clientv3.OpDelete(cc.followersPrefix+Sep+follower),
		clientv3.OpPut(cc.versionPrefix, string(conv.Int32ToBytes(int32(new_version)))),
	).Commit()

	if err != nil {
		return err
	}
	if !responses.Succeeded {
		return common.ErrVersionBehind
	}
	return nil
}
