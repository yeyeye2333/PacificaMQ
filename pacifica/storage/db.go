package storage

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/yeyeye2333/PacificaMQ/internal/logger"
	"github.com/yeyeye2333/PacificaMQ/pacifica/api"
	"github.com/yeyeye2333/PacificaMQ/pkg/conv"
	"google.golang.org/protobuf/proto"
)

func NewStorage(ops *Options) (storage Storage, err error) {
	if ops.Path == "" {
		return nil, errors.New("path is empty")
	}
	db := &DB{
		ro: ops.ReadOptions,
		wo: ops.WriteOptions,
	}
	ctx, cancel := context.WithCancel(context.Background())
	db.ctx, db.cancel = ctx, cancel
	db.minBytesPerBatch, db.maxWaitPerBatch = ops.minBytesPerBatch, ops.maxWaitPerBatch

	dbOps := ops.Options
	blockCache := grocksdb.NewLRUCache(ops.BlockCacheSize)
	tableOps := ops.TableOptions
	tableOps.SetBlockCache(blockCache)
	dbOps.SetBlockBasedTableFactory(tableOps)

	db.db, err = grocksdb.OpenDb(dbOps, ops.Path)
	if err != nil {
		return nil, err
	}

	db.appendCh = make(chan struct{}, 1)
	db.appendQueue = make([]appendEntry, 0, 20)
	go func() {
		timer := time.NewTimer(time.Millisecond * time.Duration(db.maxWaitPerBatch))
		wb := grocksdb.NewWriteBatch()

		for {
			select {
			case <-db.ctx.Done():
				return
			case <-db.appendCh:
			case <-timer.C:
			}

			getEntrys := func() []appendEntry {
				bytes := uint64(0)
				entrys := []appendEntry{}
				db.appendMu.Lock()
				defer db.appendMu.Unlock()
				for i := range db.appendQueue {
					entrys = append(entrys, db.appendQueue[i])
					for _, entry := range db.appendQueue[i].entrys {
						bytes += uint64(len(entry.GetData()))
					}

					if bytes >= db.minBytesPerBatch {
						break
					}
				}
				db.appendBytes -= bytes
				db.appendQueue = db.appendQueue[len(entrys):]
				if len(db.appendQueue) == 0 && cap(db.appendQueue) == 0 {
					db.appendQueue = make([]appendEntry, 0, 10)
				}
				return entrys
			}
			for {
				entrys := getEntrys()
				if len(entrys) == 0 {
					break
				}
				var success []bool
				for termIndex := range entrys {
					wb.SetSavePoint()
					success = append(success, true)

					for _, entry := range entrys[termIndex].entrys {
						index := entry.GetIndex()
						entry.Index = nil
						data, err := proto.Marshal(entry)
						if err != nil {
							logger.Errorf("proto marshal entry failed: %v", err)
							wb.RollbackToSavePoint()
							success[termIndex] = false
							break
						}
						wb.Put(conv.Uint64ToBytes(index), data)
					}
				}
				if err := db.db.Write(db.wo, wb); err != nil {
					logger.Errorf("write to db failed: %v", err)
					for i := range success {
						success[i] = false
					}
				}
				wb.Clear()

				for i := range success {
					entrys[i].notifyCh <- success[i]
				}
			}

			timer.Reset(time.Millisecond * time.Duration(db.maxWaitPerBatch))
		}
	}()
	return db, nil
}

type appendEntry struct {
	entrys   []*api.Entry
	notifyCh chan bool
}

type DB struct {
	ctx    context.Context
	cancel context.CancelFunc
	db     *grocksdb.DB

	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions

	minBytesPerBatch uint64
	maxWaitPerBatch  uint32

	appendCh    chan struct{}
	appendMu    sync.Mutex
	appendQueue []appendEntry
	appendBytes uint64
}

func (db *DB) Save(entrys []*api.Entry) error {
	notifyCh := make(chan bool, 1)
	db.appendMu.Lock()
	db.appendQueue = append(db.appendQueue, appendEntry{
		entrys:   entrys,
		notifyCh: notifyCh,
	})
	for _, entry := range entrys {
		db.appendBytes += uint64(len(entry.GetData()))
	}
	if db.appendBytes >= db.minBytesPerBatch {
		select {
		case db.appendCh <- struct{}{}:
		default:
		}
	}
	db.appendMu.Unlock()

	success := <-notifyCh
	if !success {
		return errors.New("append entry failed")
	}
	return nil
}
func (db *DB) Release(begin uint64, end uint64) error {
	err := db.db.DeleteRangeCF(db.wo, db.db.GetDefaultColumnFamily(), conv.Uint64ToBytes(begin), conv.Uint64ToBytes(end))
	if err != nil {
		return err
	}
	return nil
}
func (db *DB) Load(index uint64) (*api.Entry, error) {
	value, err := db.db.Get(db.ro, conv.Uint64ToBytes(index))
	defer value.Free()
	if err != nil {
		return nil, err
	}
	if len(value.Data()) == 0 {
		return nil, nil
	}

	entry := &api.Entry{}
	err = proto.Unmarshal(value.Data(), entry)
	if err != nil {
		return nil, err
	}
	entry.Index = &index
	return entry, nil
}
func (db *DB) LoadMin() (*api.Entry, error) {
	it := db.db.NewIterator(db.ro)
	defer it.Close()
	entry := &api.Entry{}
	it.SeekToFirst()
	if !it.Valid() {
		index := uint64(0)
		entry.Index = &index
		return entry, nil
	}

	key := it.Key().Data()
	value := it.Value().Data()
	defer it.Key().Free()
	defer it.Value().Free()
	index := conv.BytesToUint64(key)
	err := proto.Unmarshal(value, entry)
	if err != nil {
		return nil, err
	}
	entry.Index = &index
	return entry, nil
}
func (db *DB) LoadMax() (*api.Entry, error) {
	it := db.db.NewIterator(db.ro)
	defer it.Close()
	entry := &api.Entry{}
	it.SeekToLast()
	if !it.Valid() {
		index := uint64(0)
		entry.Index = &index
		return entry, nil
	}

	key := it.Key().Data()
	value := it.Value().Data()
	defer it.Key().Free()
	defer it.Value().Free()
	index := conv.BytesToUint64(key)
	err := proto.Unmarshal(value, entry)
	if err != nil {
		return nil, err
	}
	entry.Index = &index
	return entry, nil
}
func (db *DB) MoreLoad(index uint64, num uint64) ([]*api.Entry, error) {
	it := db.db.NewIterator(db.ro)
	defer it.Close()
	curNum := uint64(0)
	entries := []*api.Entry{}
	for it.Seek(conv.Uint64ToBytes(index)); it.Valid() && curNum < num; it.Next() {
		key := it.Key()
		defer key.Free()
		value := it.Value()
		defer value.Free()
		entry := &api.Entry{}
		err := proto.Unmarshal(value.Data(), entry)
		if err != nil {
			logger.Errorf("proto unmarshal entry failed: %v", err)
		}
		index := conv.BytesToUint64(key.Data())
		entry.Index = &index
		entries = append(entries, entry)
		curNum++
	}
	return entries, nil
}

func (db *DB) Close() {
	db.cancel()
	db.ro.Destroy()
	db.wo.Destroy()
	db.db.Close()
}
