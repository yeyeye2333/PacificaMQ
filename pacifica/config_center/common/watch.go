package common

type ConfigWatcher interface {
	Process(WatchEvent)
}

type EventType int

const (
	EventUpdateVersion EventType = iota

	EventUpdateLeader

	EventAddFollower
	EventDelFollower
)

type WatchEvent struct {
	Value interface{}
	Event EventType
}
