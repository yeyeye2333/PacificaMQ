package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/yeyeye2333/PacificaMQ/api/storage_info"
)

func TestBasicSetAndGet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s, err := NewStorage(ctx, NewOptions(WithPath("./db")))
	if err != nil {
		t.Error(err)
	}
	defer cancel()

	appendTests := []struct {
		msg               [][]byte
		producerID        *storage_info.ProducerID
		lastPacificaIndex uint64
		wantedIndex       uint64
	}{
		{
			msg:               [][]byte{[]byte("hello"), []byte("world")},
			producerID:        getProducerID(1, 1),
			lastPacificaIndex: 1,
			wantedIndex:       1,
		}, {
			msg:               [][]byte{[]byte("hello"), []byte("world")},
			producerID:        getProducerID(1, 2),
			lastPacificaIndex: 1,
			wantedIndex:       3,
		},
	}

	for _, test := range appendTests {
		indexCh := s.AppendMessages(test.msg, test.producerID, test.lastPacificaIndex)
		index := <-indexCh
		if index == 0 {
			t.Error("append failed")
		}
		t.Log("got index", index)
		if index != test.wantedIndex {
			t.Errorf("wanted index %d", test.wantedIndex)
		}
	}

	msgs, err := s.GetMessage(3, 100)
	if err != nil || len(msgs) != 2 || *msgs[1].Index != 4 {
		t.Error(err)
	}
	if bytes.Equal(msgs[0].GetData(), []byte("hello")) != true {
		t.Errorf("data not equal,msg:%v wanted:%v", msgs[0].GetData(), []byte("hello"))
	}
	m, _ := s.GetProducerIDs()
	if v, ok := m[1]; !ok || v != 2 {
		t.Error("producer id not equal")
	}
}

func getProducerID(id, seq uint64) *storage_info.ProducerID {
	return &storage_info.ProducerID{ID: &id, Sequence: &seq}
}
