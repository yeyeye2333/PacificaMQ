package common

type Status = int8
type Index = uint64
type Verson = uint64
type NodeID = string

const (
	Leader Status = iota
	Follower
	Learner
)
