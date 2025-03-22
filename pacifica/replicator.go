package pacifica

import (
	. "github.com/yeyeye2333/PacificaMQ/pacifica/common"
)

type replicator struct {
	// TODO: implement replicator
	commitIndex Index

	//leader易失性
	// nextIndex[],matchIndex[](待改),isbeat
	// follower易失性
	//  leader_lastindex
}
