package storage

import (
	"bytes"
	"testing"

	"github.com/yeyeye2333/PacificaMQ/api/storage_info"
)

func TestBasicSetAndGet(t *testing.T) {
	s, err := NewStorage(*NewOptions(WithPath("./db")))
	if err != nil {
		t.Error(err)
	}
	defer s.Close()

	appendTests := []struct {
		msg               *storage_info.ReplicativeData
		producerID        *storage_info.ProducerID
		lastPacificaIndex uint64
		wantedIndex       uint64
	}{
		{
			msg:               &storage_info.ReplicativeData{Data: [][]byte{[]byte("hello"), []byte("world")}},
			producerID:        getProducerID(1, 1),
			lastPacificaIndex: 1,
			wantedIndex:       1,
		}, {
			msg:               &storage_info.ReplicativeData{Data: [][]byte{[]byte("hello"), []byte("world")}},
			producerID:        getProducerID(1, 2),
			lastPacificaIndex: 1,
			wantedIndex:       3,
		},
	}

	for _, test := range appendTests {
		index, err := s.AppendMessages(test.msg, test.producerID, test.lastPacificaIndex)
		if err != nil {
			t.Error(err)
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
}

func getProducerID(id, seq uint64) *storage_info.ProducerID {
	return &storage_info.ProducerID{ID: &id, Sequence: &seq}
}
