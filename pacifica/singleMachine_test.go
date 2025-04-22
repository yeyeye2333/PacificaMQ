package pacifica

import (
	"bytes"
	"context"
	"testing"

	"github.com/yeyeye2333/PacificaMQ/internal/logger"
	"github.com/yeyeye2333/PacificaMQ/pacifica/storage"
)

func TestSingleMachine(t *testing.T) {
	node, err := NewNode(context.Background(), NewOptions(WithSnapshot(&Snapshot{}),
		WithStorage(storage.WithPath("./raft"))))
	if err != nil {
		t.Fatal(err)
	}
	node.Info("start")

	applyTests := []struct {
		data []byte
	}{
		{[]byte("hello")},
		{[]byte("world")},
	}

	applyCh := node.ApplyCh()
	finishCh := make(chan struct{}, 1)
	go func() {
		num := 0
		for data := range applyCh {
			if !bytes.Equal(data.Entries[0].GetData(), applyTests[num].data) {
				t.Error("apply data error")
			}
			t.Log("apply data:", data)
			num++
			if num == 2 {
				finishCh <- struct{}{}
			}
		}
	}()

	for _, test := range applyTests {
		_, _, err := node.Apply(test.data)
		if err != nil {
			t.Error(err)
		}
	}
	<-finishCh
	node.close()
}

type Snapshot struct {
	data []byte
}

func (s *Snapshot) Write(uint64) error {
	return nil
}

func (s *Snapshot) Read() ([]byte, error) {
	return []byte("snapshot"), nil
}

func (s *Snapshot) Install(data []byte) error {
	s.data = data
	logger.Info("Installing snapshot:", s.data)
	return nil
}
