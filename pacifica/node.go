package pacifica

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	logCM "github.com/yeyeye2333/PacificaMQ/internal/logger/common"
	proto "github.com/yeyeye2333/PacificaMQ/pacifica/api"
	. "github.com/yeyeye2333/PacificaMQ/pacifica/common"
	"github.com/yeyeye2333/PacificaMQ/pacifica/storage"
	storageCM "github.com/yeyeye2333/PacificaMQ/pacifica/storage/common"
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
	logCM.Logger
	ctx        context.Context
	workCancel context.CancelFunc // 被取消前可能读取变化后的version，需验证
	// 持久性
	storage    storageCM.Storage //存储最小值为第一条log或snapshot最后一条log
	snapshoter Snapshoter

	// 易失性
	// all
	me              NodeID
	config          *configChanger
	status          Status // 状态变化：leader->learner;follower->leader;follower->learner;learner->follower // 只在work准备好后原子修改
	applyCh         chan *ApplyMsg
	commitAddCh     chan struct{}
	mu              sync.RWMutex //最外层锁
	snapLastIndex   Index
	snapLastVersion Version
	commitIndex     Index
	lastApplied     Index

	// follower/learner
	leaderLastIndex Index //原子修改

	slaveMu        sync.RWMutex
	slaveTimer     *time.Timer
	followerPeriod time.Duration
	learnerPeriod  time.Duration

	// leader
	// isbeat
	nextAddCh chan struct{}
	nextIndex Index //原子修改

	addLearnerCh     chan NodeID
	removeFollowerCh chan NodeID
	addFollowerCh    chan NodeID
	leaderMu         sync.RWMutex // 以下由leader主协程创建，销毁;子协程须处理被销毁后的变量
	followerMatchs   MatchIndex
	learnerMatchs    MatchIndex

	leaderTimers map[NodeID]*time.Timer //主要用于follower超时，leader主协程非并发使用
	leaderPeriod time.Duration
}

func NewNode(ctx context.Context, me NodeID, snapshoter Snapshoter, logger logCM.Logger) (*Node, error) {
	node := &Node{
		Logger:          logger,
		ctx:             ctx,
		me:              me,
		status:          None,
		snapLastIndex:   0,
		snapLastVersion: 0,
		commitIndex:     0,
		lastApplied:     0,
	}

	storage, err := storage.NewStorage(storageCM.WithStorage("mock"))
	if err != nil {
		return nil, err
	}
	err = storage.Start()
	if err != nil {
		return nil, err
	}
	entry, err := storage.LoadMin()
	if err != nil {
		return nil, err
	}
	if entry != nil && entry.GetIndex() != 1 {
		node.snapLastIndex = entry.GetIndex()
		node.snapLastVersion = entry.GetVersion()
	}
	entry, err = storage.LoadMax()
	if err != nil {
		return nil, err
	}
	if entry != nil {
		// node.commitIndex = entry.GetIndex()
	}
	node.storage = storage

	config := &configChanger{}
	//
	config.SetBecomeLeader(func() { node.becomeCallBack(Leader) })
	config.SetBecomeFollower(func() { node.becomeCallBack(Follower) })
	config.SetBecomeLearner(func() { node.becomeCallBack(Learner) })
	err = config.Start(me)
	if err != nil {
		return nil, err
	}
	node.config = config

	return node, nil
}

func (node *Node) Snapshot(index Index) error {
	if index > node.lastApplied {
		return fmt.Errorf("snapshot index %d is greater than last applied %d", index, node.lastApplied)
	}
	node.snapshoter.Write(index)
	return nil
}

func (node *Node) ApplyCh() <-chan *ApplyMsg {
	return node.applyCh
}

func (node *Node) Apply(msg []byte) error {
	leader, version := node.config.GetLeader()
	if leader != node.me {
		return ErrNotLeader
	} else {
		entry := &proto.Entry{}
		index := atomic.LoadUint64(&node.nextIndex)
		entry.Data = msg
		entry.Index = &index
		entry.Version = &version
		err := node.storage.Save([]*proto.Entry{entry})
		if err != nil {
			return err
		} else {
			atomic.AddUint64(&node.nextIndex, 1)
			select {
			case node.nextAddCh <- struct{}{}:
			default:
			}
			return nil
		}
	}
}

func (node *Node) doApply(ctx context.Context) {
	for {
		node.mu.Lock()
		if node.commitIndex <= node.lastApplied {
			node.mu.Unlock()
		}
		for node.commitIndex > node.lastApplied {
			msg := &ApplyMsg{}
			if node.lastApplied < node.snapLastIndex {
				msg.SnapshotLastIndex = node.snapLastIndex
				msg.SnapshotLastVersion = node.snapLastVersion
				node.lastApplied = node.snapLastIndex
				node.mu.Unlock()
				//提前解锁，防止read耗时过久
				msg.Snapshot = node.snapshoter.Read()
			} else {
				num := min(node.commitIndex-node.lastApplied, 10)
				entries, err := node.storage.MoreLoad(node.lastApplied+1, num)
				if err != nil {
					node.mu.Unlock()
					node.Errorf("load entries failed: %v", err)
					break
				} else {
					node.lastApplied += Index(len(entries))
					node.mu.Unlock()
				}
				msg.Entries = append(msg.Entries, entries...)
			}
			node.applyCh <- msg
		}

		select {
		case <-ctx.Done():
			return
		case node.commitAddCh <- struct{}{}:
		}
	}
}

func (node *Node) becomeCallBack(status Status) {
	atomic.StoreInt32(&node.status, None)

	node.workCancel()
	ctx, cancel := context.WithCancel(node.ctx)
	node.workCancel = cancel
	switch status {
	case Leader:
		go node.leaderWork(ctx)
	case Follower:
		go node.followerWork(ctx)
	case Learner:
		go node.learnerWork(ctx)
	}
}
