package common

type ConfigWatcher interface {
	Process(*WatchEvent)
}

type EventType int

const (
	EventReplaceLeader EventType = iota
	EventAddFollower
	EventRemoveFollower
)

type WatchEvent struct {
	ClusterConfig
	Type EventType
}

func (e *WatchEvent) Init() {
	e.ClusterConfig.Init()
}
