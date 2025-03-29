package pacifica

import (
	"context"
	"errors"
	"math"
	"sync/atomic"

	proto "github.com/yeyeye2333/PacificaMQ/pacifica/api"
	. "github.com/yeyeye2333/PacificaMQ/pacifica/common"
)

var (
	ErrLogConflict   = errors.New("log conflict")
	ErrVersionBehind = errors.New("version behind")
	ErrOther         = errors.New("other error")
)

func (node *Node) AppendEntries(ctx context.Context, args *proto.AppendEntriesArgs) (reply *proto.AppendEntriesReply, err error) {
	node.mu.Lock()
	defer node.mu.Unlock()

	_, curVersion := node.config.GetLeader()
	if args.GetVersion() < curVersion {
		*reply.Version = curVersion
		err = ErrVersionBehind
	} else {
		if args.GetVersion() > curVersion {
			node.config.ChangeLeader(args.GetLeaderId(), args.GetVersion())
			atomic.StoreUint64(&node.leaderLastIndex, 0)
		}

		// 尝试append日志
		if len(args.GetEntries()) > 0 {
			if args.GetPrevLogIndex() < node.snapLastIndex {
				node.Errorf("leader prev log index %d is less than snapshot last index %d", args.GetPrevLogIndex(), node.snapLastIndex)
			} else {
				entry, e := node.storage.Load(args.GetPrevLogIndex())
				if e != nil {
					node.Errorf("load entry err %v", e)
					err = ErrOther
					return
				}

				if entry == nil && args.GetPrevLogIndex() != 0 {
					//prev大于本地最后一条log
					entry, e := node.storage.LoadMax()
					if e != nil {
						node.Errorf("load max entry err %v", e)
						err = ErrOther
						return
					}
					err = ErrLogConflict
					*reply.XIndex = entry.GetIndex() + 1
				} else if args.GetPrevLogIndex() == 0 || entry.GetVersion() == args.GetPrevLogVersion() {
					node.storage.Save(args.GetEntries())
					atomic.StoreUint64(&node.leaderLastIndex, *(args.Entries[len(args.Entries)-1].Index))
				} else {
					err = ErrLogConflict
					*reply.XIndex = entry.GetIndex()
				}
			}
		}
		status := atomic.LoadInt32(&node.status)
		node.slaveMu.Lock()
		if status == Follower {
			node.slaveTimer.Reset(node.followerPeriod)
		} else if status == Learner {
			node.slaveTimer.Reset(node.learnerPeriod)
		}
		node.slaveMu.Unlock()

		if node.leaderLastIndex >= args.GetLeaderCommit() {
			if args.GetLeaderCommit() > node.commitIndex {
				node.commitIndex = args.GetLeaderCommit()
				select {
				case node.commitAddCh <- struct{}{}:
				default:
				}
			}
		}
	}
	return
}

func (node *Node) sendAppendEntries(ctx context.Context, cli proto.ReplicationClient, args *proto.AppendEntriesArgs) (reply *proto.AppendEntriesReply, err error) {
	for {
		reply, err = cli.AppendEntries(ctx, args)
		if err != ErrOther {
			break
		}
		node.Warnf("retry append entries err: %v", err)
	}

	node.leaderMu.Lock()
	timer, ok := node.leaderTimers[args.GetLeaderId()]
	if ok {
		timer.Reset(node.leaderPeriod)
	} else {
		node.Errorf("leader timer not found, nodeID: %s", args.GetLeaderId())
	}
	node.leaderMu.Unlock()

	if err != ErrVersionBehind {
		return
	} else {
		node.mu.Lock()
		defer node.mu.Unlock()
		node.config.ChangeLeader(args.GetLeaderId(), args.GetVersion())
	}
	return
}

func (node *Node) InstallSnapshot(ctx context.Context, args *proto.InstallSnapshotArgs) (reply *proto.InstallSnapshotReply, err error) {
	node.mu.Lock()
	defer node.mu.Unlock()

	_, curVersion := node.config.GetLeader()
	if args.GetVersion() < curVersion {
		*reply.Version = curVersion
		err = ErrVersionBehind
	} else {
		if args.GetVersion() > curVersion {
			node.config.ChangeLeader(args.GetLeaderId(), args.GetVersion())
			atomic.StoreUint64(&node.leaderLastIndex, 0)
		}

		entry := args.GetLastIncludedEntry()
		if entry.GetIndex() <= node.commitIndex {
			node.Warnf("snapshot last included index %d is less than commit index %d", entry.GetIndex(), node.commitIndex)
			return
		} else {
			status := atomic.LoadInt32(&node.status)
			node.slaveMu.Lock()
			if status == Follower {
				node.slaveTimer.Reset(node.followerPeriod)
			} else if status == Learner {
				node.slaveTimer.Reset(node.learnerPeriod)
			}
			node.slaveMu.Unlock()

			node.storage.Release(1, math.MaxUint64)
			node.storage.Save([]*proto.Entry{entry})
			node.snapshoter.Install(args.GetData())
			node.snapLastIndex = entry.GetIndex()
			node.snapLastVersion = entry.GetVersion()
			node.commitIndex = entry.GetIndex()
			select {
			case node.commitAddCh <- struct{}{}:
			default:
			}
		}
	}
	return
}
func (node *Node) sendInstallSnapshot(ctx context.Context, cli proto.ReplicationClient, args *proto.InstallSnapshotArgs) (reply *proto.InstallSnapshotReply, err error) {
	for {
		reply, err = cli.InstallSnapshot(ctx, args)
		//grpc cancel
		if err != ErrOther {
			break
		}
		node.Warnf("retry install snapshot err: %v", err)
	}

	node.leaderMu.Lock()
	timer, ok := node.leaderTimers[args.GetLeaderId()]
	if ok {
		timer.Reset(node.leaderPeriod)
	} else {
		node.Errorf("leader timer not found, nodeID: %s", args.GetLeaderId())
	}
	node.leaderMu.Unlock()

	if err != ErrVersionBehind {
		return
	} else {
		node.mu.Lock()
		defer node.mu.Unlock()
		node.config.ChangeLeader(args.GetLeaderId(), args.GetVersion())
	}
	return
}

func (node *Node) AddNode(ctx context.Context, args *proto.AddNodeArgs) (reply *proto.AddNodeReply, err error) {
	node.addLearnerCh <- args.GetNodeId()
	return &proto.AddNodeReply{}, nil
}

func (node *Node) sendAddNode(ctx context.Context, cli proto.AddNodeClient, args *proto.AddNodeArgs) (reply *proto.AddNodeReply, err error) {
	for {
		reply, err = cli.AddNode(ctx, args)
		if err != ErrOther {
			break
		}
		node.Warnf("retry add node err: %v", err)
	}

	return
}
