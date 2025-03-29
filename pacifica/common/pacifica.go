package common

type Status = int32
type Index = uint64
type Version = uint64
type NodeID = string

const (
	None Status = iota
	Leader
	Follower
	Learner
)
