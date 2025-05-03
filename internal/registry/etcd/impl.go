package etcd

import (
	"context"
	"sync"

	"github.com/yeyeye2333/PacificaMQ/internal/extension"
	"github.com/yeyeye2333/PacificaMQ/internal/logger"
	"github.com/yeyeye2333/PacificaMQ/internal/registry/common"
	"github.com/yeyeye2333/PacificaMQ/pkg/conv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

func init() {
	extension.SetRegistry("etcd", NewRegistry)
}

const (
	Sep = "/"
)

var NewRegistry extension.RegistryFactory = func(opts common.InternalOptions) (common.Registry, error) {
	registry := &etcdRegistry{InternalOptions: opts}

	var rootPath string
	if registry.Cluster != "" {
		rootPath = registry.NameSpace + Sep + registry.Cluster + Sep
	} else {
		rootPath = registry.NameSpace + Sep
	}
	registry.rootPath = rootPath
	registry.brokerPrefix = rootPath + "broker" + Sep + "ids" + Sep
	registry.partitionPrefix = rootPath + "broker" + Sep + "topics" + Sep
	registry.consumerPrefix = rootPath + "consumers" + Sep
	logger.Debug("etcd registry start with root path: %s, partition prefix: %s, consumer prefix: %s",
		rootPath, registry.partitionPrefix, registry.consumerPrefix)

	registry.ctx, registry.cancel = context.WithCancel(context.Background())

	cli, err := clientv3.New(clientv3.Config{
		Endpoints: registry.Address,
	})
	if err != nil {
		return nil, err
	}
	registry.cli = cli
	registry.kv = clientv3.NewKV(cli)
	registry.lease = clientv3.NewLease(cli)
	registry.watcher = clientv3.NewWatcher(cli)

	registry.leaseKeys = make(map[string]string)
	registry.subs = make(map[string]context.CancelFunc)

	if registry.TTL > 0 {
		go registry.leaseHeartBeat(registry.ctx)
	}

	return registry, nil
}

type etcdRegistry struct {
	common.InternalOptions
	rootPath        string
	brokerPrefix    string
	partitionPrefix string
	consumerPrefix  string

	ctx    context.Context
	cancel context.CancelFunc

	cli     *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher

	leaseKeys map[string]string
	leaseID   clientv3.LeaseID
	leaseLock sync.RWMutex

	subs    map[string]context.CancelFunc
	subLock sync.RWMutex
}

func (r *etcdRegistry) leaseHeartBeat(ctx context.Context) {
	for {
		leaseResp, err := r.lease.Grant(ctx, r.TTL)
		if err == nil {
			keepAliveCh, err := r.lease.KeepAlive(ctx, leaseResp.ID)
			if err == nil {
				r.leaseLock.Lock()
				r.leaseID = leaseResp.ID
				for k, v := range r.leaseKeys {
					_, err = r.kv.Put(ctx, k, v, clientv3.WithLease(leaseResp.ID))
					if err != nil {
						logger.Errorf("etcd registry reg key %s failed: %v", k, err)
						break
					}
				}
				r.leaseLock.Unlock()
				logger.Infof("etcd registry new lease ID: %d", leaseResp.ID)

				for {
					select {
					case <-ctx.Done():
						return
					case _, ok := <-keepAliveCh:
						if !ok {
							break
						}
						continue
					}
				}
			}
		}
		logger.Warnf("etcd registry lease heart beat failed: %v", err)
	}
}

// 接受指针类型
func (r *etcdRegistry) Register(node interface{}) error {
	isLease := false
	var key, value string
	var data []byte
	var err error

	switch v := node.(type) {
	case *common.PartitionInfo:
		key = r.partitionPrefix + v.TopicName + Sep + string(conv.Int32ToBytes(v.PartitionID))
		data, err = proto.Marshal(v.Status)

	case *common.ConsumerInfo:
		isLease = true

		key = r.consumerPrefix + v.GroupID + Sep + "ids" + Sep + v.ConsumerAddress
		data, err = proto.Marshal(v.SubList)

	default:
		return common.ErrNotSupport
	}
	if err != nil {
		return err
	}
	value = string(data)

	if isLease {
		r.leaseLock.Lock()
		defer r.leaseLock.Unlock()
		_, err = r.kv.Put(r.ctx, key, value, clientv3.WithLease(r.leaseID))
		if err != nil {
			return err
		}
		r.leaseKeys[key] = value
	} else {
		_, err = r.kv.Put(r.ctx, key, value)
		if err != nil {
			return err
		}
	}

	return nil
}

// 接受指针类型
func (r *etcdRegistry) UnRegister(node interface{}) error {
	isLease := false
	var key string
	switch v := node.(type) {
	case *common.PartitionInfo:
		key = r.partitionPrefix + v.TopicName + Sep + string(conv.Int32ToBytes(v.PartitionID))

	case *common.ConsumerInfo:
		isLease = true
		key = r.consumerPrefix + v.GroupID + Sep + "ids" + Sep + v.ConsumerAddress
	}

	if isLease {
		r.leaseLock.Lock()
		defer r.leaseLock.Unlock()
	}
	_, err := r.kv.Delete(r.ctx, key)
	if err != nil {
		return err
	}
	if isLease {
		delete(r.leaseKeys, key)
	}
	return nil
}

func (r *etcdRegistry) SubPartition(topic string, listener common.Listener) error {
	key := r.partitionPrefix + topic + Sep

	return r.createSub(key, listener)
}

func (r *etcdRegistry) UnSubPartition(topic string) {
	key := r.partitionPrefix + topic + Sep
	r.deleteSub(key)
}

func (r *etcdRegistry) SubConsumerGroup(groupID string, listener common.Listener) error {
	key := r.consumerPrefix + groupID + Sep + "ids" + Sep

	return r.createSub(key, listener)
}

func (r *etcdRegistry) UnSubConsumerGroup(groupID string) {
	key := r.consumerPrefix + groupID + Sep + "ids" + Sep
	r.deleteSub(key)
}

func (r *etcdRegistry) ChangeBroker(brokerInfo *common.BrokerInfo) error {
	key := r.brokerPrefix + brokerInfo.Address
	oldValue, err := proto.Marshal(brokerInfo.OldPartitions)
	if err != nil {
		return err
	}
	newValue, err := proto.Marshal(brokerInfo.NewPartitions)
	if err != nil {
		return err
	}

	realOp := []clientv3.Op{}
	if len(brokerInfo.NewPartitions.TopicName) == 0 {
		realOp = append(realOp, clientv3.OpDelete(key))
	} else {
		realOp = append(realOp, clientv3.OpPut(key, string(newValue)))
	}

	responses, err := r.kv.Txn(r.ctx).If(
		clientv3.Compare(clientv3.CreateRevision(key), "!=", 0),
	).Then(
		clientv3.OpTxn(
			[]clientv3.Cmp{
				clientv3.Compare(clientv3.Value(key), "=", string(oldValue)),
			},
			realOp,
			[]clientv3.Op{},
		),
	).Else(
		realOp...,
	).Commit()

	if err != nil {
		return err
	}
	if responses.Succeeded && !responses.Responses[0].GetResponseTxn().Succeeded {
		return common.ErrBrokerBehind
	}
	return nil
}

func (r *etcdRegistry) SubBroker(broker string, listener common.Listener) error {
	key := r.brokerPrefix + broker

	return r.createSub(key, listener)
}

func (r *etcdRegistry) UnSubBroker(broker string) {
	key := r.brokerPrefix + broker
	r.deleteSub(key)
}

func (r *etcdRegistry) GetConsumerLeader(me *common.ConsumerLeader) error {
	key := r.consumerPrefix + me.GroupID + Sep + "leader"

	responses, err := r.kv.Txn(r.ctx).If(
		clientv3.Compare(clientv3.CreateRevision(key), "!=", 0),
	).Then(
		clientv3.OpPut(key, me.Leader),
	).Commit()

	if err != nil {
		return err
	}
	if !responses.Succeeded {
		return common.ErrLeaderExists
	}
	return nil
}

func (r *etcdRegistry) SubConsumerLeader(groupID string, listener common.Listener) error {
	key := r.consumerPrefix + groupID + Sep + "leader"

	return r.createSub(key, listener)
}

func (r *etcdRegistry) UnSubConsumerLeader(groupID string) {
	key := r.consumerPrefix + groupID + Sep + "leader"
	r.deleteSub(key)
}

func (r *etcdRegistry) Close() error {
	r.cancel()
	return r.cli.Close()
}

// get后init（有数据就传入putType），删除旧订阅，监听从get的rev+1开始，go一个process监听到的事件（知道ctx取消），将cancel装入subs
func (r *etcdRegistry) createSub(key string, listener common.Listener) error {
	lis := newListener(listener, r.rootPath, r.brokerPrefix, r.partitionPrefix, r.consumerPrefix)
	getResp, err := r.kv.Get(r.ctx, key, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	lis.initialize(getResp)

	r.deleteSub(key)

	ctx, cancel := context.WithCancel(r.ctx)
	go func(ctx context.Context) {
		rev := getResp.Header.GetRevision() + 1
		for {
			watchCh := r.watcher.Watch(ctx, key, clientv3.WithPrefix(), clientv3.WithRev(rev))
			for {
				select {
				case <-ctx.Done():
					return
				case watchResp, ok := <-watchCh:
					if !ok {
						return
					}
					if watchResp.Err() != nil {
						// 非context取消异常
						logger.Warnf("etcd registry watch key %s withPrefix failed: %v", key, watchResp.Err())
						break
					}

					for _, event := range watchResp.Events {
						lis.process(event)
					}
					rev = watchResp.Header.GetRevision() + 1
				}
			}
		}
	}(ctx)

	r.subLock.Lock()
	defer r.subLock.Unlock()
	r.subs[key] = cancel

	return nil
}

// 从sub取出cancel并调用，删除subs的key
func (r *etcdRegistry) deleteSub(key string) {
	r.subLock.Lock()
	defer r.subLock.Unlock()
	if prevCancel, ok := r.subs[key]; ok {
		prevCancel()
		delete(r.subs, key)
	}
}
