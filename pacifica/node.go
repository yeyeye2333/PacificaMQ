package pacifica

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	logCM "github.com/yeyeye2333/PacificaMQ/internal/logger/common"
	proto "github.com/yeyeye2333/PacificaMQ/pacifica/api"
	. "github.com/yeyeye2333/PacificaMQ/pacifica/common"
	"github.com/yeyeye2333/PacificaMQ/pacifica/storage"
	"google.golang.org/grpc"
)

var (
	ErrNotLeader = fmt.Errorf("not leader")
)

type ApplyMsg struct {
	Entries []*proto.Entry

	Snapshot            []byte
	SnapshotLastIndex   Index
	SnapshotLastVersion Version
}

type Node struct {
	maxNumsOnce Index

	proto.UnimplementedReplicationServer
	proto.UnimplementedAddNodeServer
	grpcServer *grpc.Server
	logCM.Logger
	ctx        context.Context
	workCancel context.CancelFunc // 被取消前可能读取变化后的version，需验证
	// 持久性
	storage    storage.Storage //存储最小值为第一条log或snapshot最后一条log
	snapshoter Snapshoter

	// 易失性
	// all
	me              NodeID
	config          *configChanger
	status          Status // 状态变化：leader->learner;follower->leader;follower->learner;learner->follower // 只在work准备好后原子修改
	applyCh         chan *ApplyMsg
	commitAddCh     chan struct{}
	lastApplied     Index
	mu              sync.RWMutex //最外层锁
	snapLastIndex   Index
	snapLastVersion Version
	commitIndex     Index

	// follower/learner
	leaderLastIndex Index //原子修改

	slaveMu        sync.RWMutex
	slaveTimer     *time.Timer
	followerPeriod time.Duration
	learnerPeriod  time.Duration

	// leader
	// isbeat
	isLeasePeriod int32 //原子修改
	nextAddCh     chan struct{}
	nextIndex     Index //原子修改

	addLearnerCh     chan NodeID
	removeFollowerCh chan NodeID
	addFollowerCh    chan NodeID
	leaderMu         sync.RWMutex // 以下由leader主协程创建，销毁;子协程须处理被销毁后的变量
	followerMatchs   MatchIndex
	learnerMatchs    MatchIndex

	leaderTimers map[NodeID]*time.Timer //主要用于follower超时，leader主协程非并发使用
	leaderPeriod time.Duration

	// 回调
	onBecomeLeader func(version uint64, leader string, followers []string) error
}

func NewNode(ctx context.Context, options *Options) (*Node, error) {
	if options.Snapshot == nil {
		return nil, fmt.Errorf("snapshoter is nil")
	}
	node := &Node{
		maxNumsOnce:      options.MaxNumsOnce,
		Logger:           options.Logger,
		ctx:              ctx,
		workCancel:       func() {},
		snapshoter:       options.Snapshot,
		me:               options.Address,
		status:           None,
		applyCh:          make(chan *ApplyMsg, 1),
		commitAddCh:      make(chan struct{}, 1),
		followerPeriod:   time.Duration(options.FollowerPeriod) * time.Millisecond,
		learnerPeriod:    time.Duration(options.LearnerPeriod) * time.Millisecond,
		nextAddCh:        make(chan struct{}, 1),
		addLearnerCh:     make(chan NodeID, 1),
		removeFollowerCh: make(chan NodeID, 1),
		addFollowerCh:    make(chan NodeID, 1),
		leaderPeriod:     time.Duration(options.LeaderPeriod) * time.Millisecond,
		onBecomeLeader:   options.OnBecomeLeader,
	}
	node.Info("node address:", node.me)

	storage, err := storage.NewStorage(options.StorageOpts)
	node.storage = storage
	if err != nil {
		return nil, err
	}

	entry, err := storage.LoadMin()
	if err != nil {
		return nil, err
	}
	if entry != nil && entry.GetIndex() > 1 {
		node.snapLastIndex = entry.GetIndex()
		node.snapLastVersion = entry.GetVersion()
	}

	lis, err := net.Listen("tcp", options.Address)
	if err != nil {
		return nil, err
	}
	node.grpcServer = grpc.NewServer()
	proto.RegisterReplicationServer(node.grpcServer, node)
	proto.RegisterAddNodeServer(node.grpcServer, node)
	go node.grpcServer.Serve(lis)

	config := &configChanger{}
	node.config = config
	config.SetBecomeLeader(func() { node.becomeCallBack(Leader) })
	config.SetBecomeFollower(func() { node.becomeCallBack(Follower) })
	config.SetBecomeLearner(func() { node.becomeCallBack(Learner) })
	err = config.Start(options.Address, node.Logger, options.CcOpts)
	if err != nil {
		return nil, err
	}

	go node.doApply(node.ctx)

	return node, nil
}

func (node *Node) close() error {
	defer func() {
		node.workCancel()
	}()
	node.grpcServer.Stop()
	node.storage.Close()
	close(node.applyCh)
	return node.config.Close()
}

// 还需判断是否apply至当前任期
func (node *Node) IsLeader() uint64 {
	leader, version := node.config.GetLeader()
	if (leader == node.me) && (atomic.LoadInt32(&node.isLeasePeriod) != 0) {
		return version
	}
	return 0
}

func (node *Node) Snapshot(index Index) error {
	if index > node.lastApplied {
		return fmt.Errorf("snapshot index %d is greater than last applied %d", index, node.lastApplied)
	}
	if err := node.snapshoter.Write(index); err != nil {
		return err
	}

	if err := node.storage.Release(uint64(1), index); err != nil {
		node.Error("release failed: %v", err)
	}
	return nil
}

func (node *Node) ApplyCh() <-chan *ApplyMsg {
	return node.applyCh
}

func (node *Node) Apply(msg []byte) (Index, Version, error) {
	leader, version := node.config.GetLeader()
	if leader != node.me {
		return 0, version, ErrNotLeader
	} else {
		entry := &proto.Entry{}
		index := atomic.LoadUint64(&node.nextIndex)
		entry.Data = msg
		entry.Index = &index
		entry.Version = &version
		node.Info("save entry:", entry)
		err := node.storage.Save([]*proto.Entry{entry})
		if err != nil {
			return 0, version, err
		} else {
			atomic.AddUint64(&node.nextIndex, 1)
			select {
			case node.nextAddCh <- struct{}{}:
			default:
			}
			return entry.GetIndex(), version, nil
		}
	}
}

func (node *Node) doApply(ctx context.Context) {
	for {
		node.mu.Lock()
		if node.commitIndex > node.lastApplied {
			node.mu.Unlock()
			for node.commitIndex > node.lastApplied {
				msg := &ApplyMsg{}
				node.mu.Lock()
				if node.lastApplied < node.snapLastIndex {
					node.mu.Unlock()
					msg.SnapshotLastIndex = node.snapLastIndex
					msg.SnapshotLastVersion = node.snapLastVersion
					//提前解锁，防止read耗时过久
					snapshot, err := node.snapshoter.Read()
					if err != nil {
						node.Error("read snapshot failed: %v", err)
						break
					} else {
						node.lastApplied = node.snapLastIndex
						msg.Snapshot = snapshot
					}
				} else {
					num := min(node.commitIndex-node.lastApplied, 10)
					node.mu.Unlock()
					node.Info(node.lastApplied+1, num)
					entries, err := node.storage.MoreLoad(node.lastApplied+1, num)
					if err != nil {
						node.Errorf("load entries failed: %v", err)
						break
					} else {
						node.lastApplied += Index(len(entries))
					}
					node.Info(node.lastApplied, entries)
					msg.Entries = append(msg.Entries, entries...)
				}
				node.Info("msg:", msg)
				node.applyCh <- msg
			}
		} else {
			node.mu.Unlock()
		}

		select {
		case <-ctx.Done():
			return
		case <-node.commitAddCh:
		}
	}
}

func (node *Node) becomeCallBack(status Status) {
	atomic.StoreInt32(&node.status, None)

	node.workCancel()
	ctx, cancel := context.WithCancel(node.ctx)
	node.workCancel = cancel
	prePare := make(chan struct{})
	switch status {
	case Leader:
		leader, version := node.config.GetLeader()
		go node.onBecomeLeader(version, leader, node.config.GetFollowers()) ///////
		go node.leaderWork(ctx, prePare)
	case Follower:
		go node.followerWork(ctx, prePare)
	case Learner:
		go node.learnerWork(ctx, prePare)
	}
	<-prePare
}
