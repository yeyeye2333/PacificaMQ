package pacifica

import (
	"context"
	"sync/atomic"
	"time"

	proto "github.com/yeyeye2333/PacificaMQ/pacifica/api"
	. "github.com/yeyeye2333/PacificaMQ/pacifica/common"
	grpc "google.golang.org/grpc"
)

func (node *Node) leaderWork(ctx context.Context, prePare chan struct{}) {
	node.Info("leader work start")
	atomic.StoreInt32(&node.isLeasePeriod, 1)
	entry, err := node.storage.LoadMax()
	if err != nil {
		node.Errorf("load max entry failed: %v", err)
		return
	}
	atomic.StoreUint64(&node.nextIndex, entry.GetIndex()+1)

	node.leaderMu.Lock()
	node.leaderTimers = make(map[NodeID]*time.Timer)
	node.followerMatchs.Init()
	node.learnerMatchs.Init()
	node.leaderMu.Unlock()

	nextIndexChMap := make(map[NodeID]chan struct{})
	ctxMap := make(map[NodeID]context.CancelFunc)

	followers := node.config.GetFollowers()

	deleteFunc := func(nodeID NodeID) {
		node.leaderMu.Lock()
		node.followerMatchs.RemoveNode(nodeID)
		node.learnerMatchs.RemoveNode(nodeID)
		node.leaderMu.Unlock()
		if cancel, ok := ctxMap[nodeID]; ok {
			cancel()
			delete(node.leaderTimers, nodeID)
			delete(ctxMap, nodeID)
			delete(nextIndexChMap, nodeID)
		}
	}

	createFunc := func(nodeID NodeID, status Status) {
		if _, ok := ctxMap[nodeID]; ok {
			deleteFunc(nodeID)
		}

		node.leaderMu.Lock()
		if status == Follower {
			node.followerMatchs.AddNode(NodeItem{NodeID: nodeID, AppendIndex: 0})
			node.leaderTimers[nodeID] = time.NewTimer(node.leaderPeriod)
		} else if status == Learner {
			node.learnerMatchs.AddNode(NodeItem{NodeID: nodeID, AppendIndex: 0})
		}
		node.leaderMu.Unlock()

		childCtx, cancel := context.WithCancel(ctx)
		nextIndexChMap[nodeID] = make(chan struct{}, 1)
		ctxMap[nodeID] = cancel
		go node.slaveSender(childCtx, Follower, nodeID, node.me, nextIndexChMap[nodeID])
	}

	for _, follower := range followers {
		createFunc(follower, Follower)
	}

	minCh := node.followerMatchs.MinCh()
	// 防止leader崩溃后影响一致性，延迟更改日志
	needAddFollowers := make([]NodeID, 0)

	atomic.StoreInt32(&node.status, Leader)
	prePare <- struct{}{}
	close(prePare)
	for {
		select {
		case <-ctx.Done():
			return
		case <-node.nextAddCh:
			if len(nextIndexChMap) == 0 {
				//单节点
				node.mu.Lock()
				node.commitIndex = atomic.LoadUint64(&node.nextIndex) - 1
				node.mu.Unlock()
				select {
				case node.commitAddCh <- struct{}{}:
				default:
				}
			} else {
				for _, ch := range nextIndexChMap {
					select {
					case ch <- struct{}{}:
					default:
					}
				}
			}
		case <-minCh:
			node.leaderMu.RLock()
			min := node.followerMatchs.GetMin()
			node.leaderMu.RUnlock()
			node.mu.Lock()
			if min > node.commitIndex {
				node.commitIndex = min
				node.mu.Unlock()
				select {
				case node.commitAddCh <- struct{}{}:
				default:
				}

				leader, newVersion := node.config.GetLeader()
				if leader != node.me {
					node.Errorf("leader is not me, leader:%s, me:%s", leader, node.me)
					return
				}
				newVersion++
				if len(needAddFollowers) > 0 {
					for _, nodeID := range needAddFollowers {
						err = node.config.configCenter.AddFollower(nodeID, newVersion)
						if err != nil {
							node.Errorf("add follower failed: %v", err)
							if err == ErrVersionBehind {
								return
							}
						}
					}
				}
			} else {
				node.mu.Unlock()
			}
		case nodeID := <-node.addLearnerCh:
			createFunc(nodeID, Learner)
		case nodeID := <-node.removeFollowerCh:
			deleteFunc(nodeID)
			leader, newVersion := node.config.GetLeader()
			if leader != node.me {
				node.Errorf("leader is not me, leader:%s, me:%s", leader, node.me)
				return
			}
			newVersion++
			err = node.config.configCenter.RemoveFollower(nodeID, newVersion)
			if err != nil {
				node.Errorf("add follower failed: %v", err)
				if err == ErrVersionBehind {
					return
				}
			} else {
				atomic.AddInt32(&node.isLeasePeriod, 1)
			}
		case nodeID := <-node.addFollowerCh:
			deleteFunc(nodeID)
			createFunc(nodeID, Follower)
			needAddFollowers = append(needAddFollowers, nodeID)
		}
	}
}

func (node *Node) slaveSender(ctx context.Context, status Status, nodeID NodeID, me NodeID,
	nextIndexCh chan struct{}) {
	conn, err := grpc.Dial(nodeID)
	if err != nil {
		node.Errorf("slave dial %s failed: %v", nodeID, err)
		return
	}
	cli := proto.NewReplicationClient(conn)
	defer conn.Close()

	//设置定时心跳和lease period判断是否需要删除follower
	heartbeat := time.NewTimer(node.leaderPeriod / 3)
	node.leaderMu.Lock()
	timer, ok := node.leaderTimers[nodeID]
	if ok {
		timer.Reset(node.leaderPeriod)
	} else {
		node.Errorf("leader timer not found, nodeID:%s", nodeID)
	}
	node.leaderMu.Unlock()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				atomic.StoreInt32(&node.isLeasePeriod, 0)
				node.removeFollowerCh <- nodeID
				return
			case <-heartbeat.C:
				leader, version := node.config.GetLeader()
				if leader != me {
					node.Errorf("leader is not me, leader:%s, me:%d", leader, me)
					return
				}
				args := &proto.AppendEntriesArgs{}
				node.mu.RLock()
				args.LeaderCommit = &node.commitIndex
				node.mu.RUnlock()
				args.LeaderId = &me
				args.Version = &version
				heartbeat.Reset(node.leaderPeriod / 3)
				node.sendAppendEntries(ctx, cli, args)
			}
		}
	}()

	var nextIndex Index
	entry, err := node.storage.LoadMax()
	if err != nil {
		node.Errorf("load max log failed: %v", err)
	} else {
		// 尝试append最后一条持久化日志恢复commitindex
		nextIndex = *entry.Index
	}

	matchIndex := Index(0)
	for {
		leaderNextIndex := atomic.LoadUint64(&node.nextIndex)
		for leaderNextIndex > nextIndex {
			if nextIndex > node.snapLastIndex {
				leader, version := node.config.GetLeader()
				if leader != me {
					node.Errorf("leader is not me, leader:%d, me:%d", leader, me)
					return
				}
				args := &proto.AppendEntriesArgs{}
				node.mu.RLock()
				args.LeaderCommit = &node.commitIndex
				node.mu.RUnlock()
				args.LeaderId = &me
				args.Version = &version

				var num Index
				if matchIndex == 0 {
					num = 1
				} else {
					num = min(leaderNextIndex-nextIndex, node.maxNumsOnce)
				}
				//
				entries, err := node.storage.MoreLoad(nextIndex-1, num+1)
				if err != nil {
					node.Errorf("more load log failed: %v", err)
					break
				}
				args.PrevLogIndex = entries[0].Index
				args.PrevLogVersion = entries[0].Version
				args.Entries = entries[1:]
				heartbeat.Reset(node.leaderPeriod / 3)
				reply, err := node.sendAppendEntries(ctx, cli, args)
				if ok {
					timer.Reset(node.leaderPeriod)
				}
				if err == nil {
					nextIndex = nextIndex + num
					matchIndex = nextIndex - 1
					node.leaderMu.Lock()
					if status == Follower {
						item := &NodeItem{NodeID: nodeID, AppendIndex: matchIndex}
						node.followerMatchs.UpdateNode(*item)
					} else if status == Learner {
						item := &NodeItem{NodeID: nodeID, AppendIndex: matchIndex}
						node.learnerMatchs.UpdateNode(*item)
					}
					node.leaderMu.Unlock()
				} else if err == ErrLogConflict {
					nextIndex = reply.GetXIndex()
				} else {
					node.Errorf("send append entries to %d failed: %v", nodeID, err)
					break
				}
			} else {
				leader, version := node.config.GetLeader()
				if leader != me {
					node.Errorf("leader is not me, leader:%d, me:%d", leader, me)
					return
				}
				args := &proto.InstallSnapshotArgs{}
				args.LeaderId = &me
				args.Version = &version
				entry, err := node.storage.Load(node.snapLastIndex)
				if err != nil {
					node.Errorf("load log failed: %v", err)
					break
				}
				args.LastIncludedEntry = entry
				args.Data, err = node.snapshoter.Read()
				if err != nil {
					node.Errorf("read snapshot failed: %v", err)
					break
				}
				heartbeat.Reset(node.leaderPeriod / 3)
				_, err = node.sendInstallSnapshot(ctx, cli, args)
				if ok {
					timer.Reset(node.leaderPeriod)
				}
				if err == nil {
					nextIndex = node.snapLastIndex + 1
					matchIndex = node.snapLastIndex
					node.leaderMu.Lock()
					if status == Follower {
						item := &NodeItem{NodeID: nodeID, AppendIndex: matchIndex}
						node.followerMatchs.UpdateNode(*item)
					} else if status == Learner {
						item := &NodeItem{NodeID: nodeID, AppendIndex: matchIndex}
						node.learnerMatchs.UpdateNode(*item)
					}
					node.leaderMu.Unlock()
				} else {
					node.Errorf("send install snapshot to %d failed: %v", nodeID, err)
					break
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-nextIndexCh:
			continue
		}
	}
}

func (node *Node) followerWork(ctx context.Context, prePare chan struct{}) {
	node.Info("follower work start")
	atomic.StoreUint64(&node.leaderLastIndex, 0)
	node.slaveMu.Lock()
	node.slaveTimer = time.NewTimer(node.followerPeriod)
	node.slaveMu.Unlock()
	oldLader, _ := node.config.GetLeader()

	atomic.StoreInt32(&node.status, Follower)
	prePare <- struct{}{}
	close(prePare)
	for {
		select {
		case <-node.slaveTimer.C:
			// 尝试替换leader
			leader, newVersion := node.config.GetLeader()
			if leader != oldLader {
				oldLader = leader
				node.slaveTimer.Reset(node.followerPeriod)
				continue
			}
			newVersion++
			err := node.config.configCenter.ReplaceLeader(node.me, newVersion)
			if err != nil {
				node.Errorf("replace leader failed: %v", err)
			}
			node.config.ChangeLeader(node.me, newVersion) // config内部回调
		case <-ctx.Done():
			return
		}
	}
}

func (node *Node) learnerWork(ctx context.Context, prePare chan struct{}) {
	node.Info("learner work start")
	atomic.StoreUint64(&node.leaderLastIndex, 0)
	node.slaveMu.Lock()
	node.slaveTimer = time.NewTimer(node.followerPeriod)
	node.slaveMu.Unlock()
	oldLeader, oldversion := node.config.GetLeader()

	conn, err := grpc.Dial(oldLeader)
	prePare <- struct{}{}
	close(prePare)
	if err != nil {
		node.Errorf("leader dial %s failed: %v", oldLeader, err)
		return
	}
	cli := proto.NewAddNodeClient(conn)
	defer conn.Close()
	args := &proto.AddNodeArgs{}
	args.NodeId = &node.me
	node.sendAddNode(ctx, cli, args)

	atomic.StoreInt32(&node.status, Learner)
	for {
		select {
		case <-node.slaveTimer.C:
			// 尝试替换leader
			leader, version := node.config.GetLeader()
			if leader == oldLeader && version == oldversion {
				node.slaveTimer.Reset(node.followerPeriod)
				continue
			} else if leader == oldLeader {
				node.sendAddNode(ctx, cli, args)
			} else {
				conn.Close()
				conn, err = grpc.Dial(leader)
				if err != nil {
					node.Errorf("leader dial %s failed: %v", oldLeader, err)
					return
				}
				cli = proto.NewAddNodeClient(conn)
			}
			oldLeader = leader
			oldversion = version
		case <-ctx.Done():
			return
		}
	}
}
