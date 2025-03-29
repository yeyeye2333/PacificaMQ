package common

import (
	"container/heap"
)

type MatchIndex struct {
	matchMap matchMap
	match    matchQueue
	min      Index
	minCh    chan struct{}
}

type NodeItem struct {
	NodeID      NodeID
	AppendIndex Index
}

// 可用于重置
func (m *MatchIndex) Init(nodes ...NodeItem) {
	m.matchMap = make(matchMap)
	m.match = make(matchQueue, len(nodes))
	m.min = 0
	m.minCh = make(chan struct{}, 1)
	i := 0
	for _, node := range nodes {
		m.match[i] = &matchItem{node.AppendIndex, i}
		m.matchMap[node.NodeID] = m.match[i]
		i++
	}
	heap.Init(&m.match)
	if m.getMin() > m.min {
		m.min = m.getMin()
		select {
		case m.minCh <- struct{}{}:
		default:
		}
	}
}

func (m *MatchIndex) AddNode(node NodeItem) error {
	if _, ok := m.matchMap[node.NodeID]; ok {
		return nil
	}
	m.matchMap[node.NodeID] = &matchItem{node.AppendIndex, -1}
	heap.Push(&m.match, m.matchMap[node.NodeID])
	if m.getMin() > m.min {
		m.min = m.getMin()
		select {
		case m.minCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func (m *MatchIndex) RemoveNode(nodeID NodeID) {
	if _, ok := m.matchMap[nodeID]; ok {
		heap.Remove(&m.match, m.matchMap[nodeID].heapIndex)
		delete(m.matchMap, nodeID)
	}
}

func (m *MatchIndex) UpdateNode(node NodeItem) {
	if _, ok := m.matchMap[node.NodeID]; ok {
		m.matchMap[node.NodeID].appendIndex = node.AppendIndex
		heap.Fix(&m.match, m.matchMap[node.NodeID].heapIndex)
		if m.getMin() > m.min {
			m.min = m.getMin()
			select {
			case m.minCh <- struct{}{}:
			default:
			}
		}
	}
}

func (m *MatchIndex) MinCh() chan struct{} {
	return m.minCh
}

func (m *MatchIndex) GetMin() Index {
	return m.min
}

func (m *MatchIndex) getMin() Index {
	if len(m.match) == 0 {
		return 0
	}
	return m.match[0].appendIndex
}

type matchItem struct {
	appendIndex Index
	heapIndex   int
}

type matchMap map[NodeID]*matchItem

type matchQueue []*matchItem

func (q matchQueue) Len() int {
	return len(q)
}

func (q matchQueue) Less(i, j int) bool {
	return q[i].appendIndex < q[j].appendIndex
}

func (q matchQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].heapIndex = i
	q[j].heapIndex = j
}

func (q *matchQueue) Push(x interface{}) {
	index := len(*q)
	item := x.(*matchItem)
	item.heapIndex = index
	*q = append(*q, item)
}

func (q *matchQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	item.heapIndex = -1 // for safety
	*q = old[0 : n-1]
	return item
}

type Interface heap.Interface
