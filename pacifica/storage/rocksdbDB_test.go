package storage

import (
	"bytes"
	"testing"

	"github.com/yeyeye2333/PacificaMQ/pacifica/api"
)

func TestBasicSetAndGet(t *testing.T) {
	s, err := NewStorage(NewOptions(WithPath("./db")))
	if err != nil {
		t.Error(err)
	}
	defer s.Close()

	max, err := s.LoadMax()
	if err != nil {
		t.Error(err)
	}
	if max.GetIndex() != 0 {
		t.Error("max!= nil")
	}
	min, err := s.LoadMin()
	if err != nil {
		t.Error(err)
	}
	if min.GetIndex() != 0 {
		t.Error("min!= nil")
	}

	entriesTests := []struct {
		index   uint64
		version uint64
		data    []byte
	}{
		{
			index:   1,
			version: 1,
			data:    []byte("hello world1"),
		},
		{
			index:   2,
			version: 1,
			data:    []byte("hello world2"),
		},
	}

	for index := range entriesTests {
		entry := &api.Entry{
			Index:   &entriesTests[index].index,
			Version: &entriesTests[index].version,
			Data:    entriesTests[index].data,
		}
		err = s.Save([]*api.Entry{entry})
		if err != nil {
			t.Error(err)
		}
	}

	datas, err := s.MoreLoad(1, 2)
	if err != nil {
		t.Error(err)
	}
	if len(datas) != 2 {
		t.Error("len(datas)!= 2")
	}
	for index := range entriesTests {
		if !bytes.Equal(datas[index].Data, entriesTests[index].data) {
			t.Errorf("datas[index].Data: %v != entriesTests[index].data: %v", datas[index].Data, entriesTests[index].data)
		}
	}

	max, err = s.LoadMax()
	if err != nil {
		t.Error(err)
	}
	if max.GetIndex() != 2 {
		t.Error("max.GetIndex() != 2")
	}
}
