package pacifica

import (
	. "github.com/yeyeye2333/PacificaMQ/pacifica/common"
	"github.com/yeyeye2333/PacificaMQ/pacifica/storage"
	storageCM "github.com/yeyeye2333/PacificaMQ/pacifica/storage/common"
)

type Node struct {
	// 持久性
	storage storageCM.Storage

	// 易失性
	config     configChanger
	replicator replicator // 只支持leader>>follower;follower>>learner

	me          NodeID
	status      Status // 状态变化：leader->learner;follower->leader;follower->learner;learner->follower
	commitIndex Index
	lastApplied Index
	nextIndex   Index
}

func NewNode() {
	storage.NewStorage(storageCM.WithStorage("mock"))
}
