package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/yeyeye2333/PacificaMQ/api/storage_info"
	"github.com/yeyeye2333/PacificaMQ/internal/logger"
	"github.com/yeyeye2333/PacificaMQ/pkg/conv"
	"google.golang.org/protobuf/proto"
)

const (
	MsgCfName  = "default"
	MetaCfName = "meta"

	MsgCfIndex  = 0
	MetaCfIndex = 1
)

const (
	MetaProducerIDPrefix  = "ProducerID"
	MetaConsumedIndex     = "ConsumedIndex"
	MetaLastPacificaIndex = "LastPacificaIndex"
)

func NewStorage(ctx context.Context, ops *Options) (storage Storage, err error) {
	if ops.Path == "" {
		return nil, errors.New("path is empty")
	}
	db := &DB{
		ctx: ctx,
		ro:  ops.ReadOptions,
		wo:  ops.WriteOptions,
	}

	db.minBytesPerBatch, db.maxWaitPerBatch = ops.minBytesPerBatch, ops.maxWaitPerBatch

	dbOps := ops.Options

	blockCache := grocksdb.NewLRUCache(ops.BlockCacheSize)

	msgCfOps := ops.Options
	msgTableOps := ops.TableOptions
	msgTableOps.SetBlockCache(blockCache)
	msgCfOps.SetBlockBasedTableFactory(msgTableOps)
	fifoo := ops.FifoOPtions
	fifoo.SetMaxTableFilesSize(ops.MaxTableFilesSize + (msgCfOps.GetWriteBufferSize() * uint64(msgCfOps.GetMinWriteBufferNumberToMerge())))
	msgCfOps.SetFIFOCompactionOptions(fifoo)

	metaCfOps := grocksdb.NewDefaultOptions()
	metaTableOps := grocksdb.NewDefaultBlockBasedTableOptions()
	metaTableOps.SetBlockCache(blockCache)
	metaCfOps.SetBlockBasedTableFactory(metaTableOps)

	cfNames := []string{MsgCfName, MetaCfName}
	cfOps := []*grocksdb.Options{msgCfOps, metaCfOps}
	db.db, db.cfHandles, err = grocksdb.OpenDbColumnFamilies(dbOps, ops.Path, cfNames, cfOps)
	if err != nil {
		return nil, err
	}

	db.appendCh = make(chan struct{}, 1)
	db.appendQueue = make([]appendEntry, 0, 10)
	go func() {
		timer := time.NewTimer(time.Millisecond * time.Duration(db.maxWaitPerBatch))
		wb := grocksdb.NewWriteBatch()
		nextIndex := uint64(1)
		func() {
			// 获取最后一条消息的索引
			it := db.db.NewIteratorCF(db.ro, db.cfHandles[MsgCfIndex])
			defer it.Close()
			it.SeekToLast()
			if it.Valid() {
				key := it.Key()
				nextIndex = conv.BytesToUint64(key.Data()) + 1
				key.Free()
			}
		}()

		for {
			select {
			case <-db.ctx.Done():
				db.close()
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
					for _, data := range db.appendQueue[i].data {
						bytes += uint64(len(data))
					}

					if bytes >= db.minBytesPerBatch {
						break
					}
				}
				db.appendBytes -= bytes
				db.appendQueue = db.appendQueue[len(entrys):]
				if len(db.appendQueue) == 0 && cap(db.appendQueue) == 0 {
					db.appendQueue = make([]appendEntry, 0, 20)
				}
				return entrys
			}
			for {
				entrys := getEntrys()
				if len(entrys) == 0 {
					break
				}
				var indexs []uint64
				now := time.Now().Unix()
				for termIndex := range entrys {
					wb.SetSavePoint()
					indexs = append(indexs, nextIndex)

					nextTerm := false
					oldIndex := nextIndex
					for dataIndex := range entrys[termIndex].data {
						record := &storage_info.Record{
							Data: entrys[termIndex].data[dataIndex],
						}
						recordData, err := proto.Marshal(record)
						if err != nil {
							logger.Errorf("proto marshal record failed: %v", err)
							wb.RollbackToSavePoint()
							indexs[len(indexs)-1] = 0
							nextIndex = oldIndex
							nextTerm = true
							break
						}
						wb.PutCF(db.cfHandles[MsgCfIndex], conv.Uint64ToBytes(nextIndex), recordData)
						nextIndex++
					}
					if nextTerm {
						nextTerm = false
						continue
					}

					producerID := &storage_info.ProducerID{
						Sequence:  &entrys[termIndex].producerSeq,
						Timestamp: &now,
					}
					producerIDData, err := proto.Marshal(producerID)
					if err != nil {
						logger.Errorf("proto marshal producerID failed: %v", err)
						wb.RollbackToSavePoint()
						indexs[len(indexs)-1] = 0
						nextIndex = oldIndex
						continue
					}
					wb.PutCF(db.cfHandles[MetaCfIndex], append([]byte(MetaProducerIDPrefix), conv.Uint64ToBytes(entrys[termIndex].producerID)...), producerIDData)
				}
				wb.PutCF(db.cfHandles[MetaCfIndex], []byte(MetaLastPacificaIndex), conv.Uint64ToBytes(entrys[len(entrys)-1].LastPacificaIndex))
				if err := db.db.Write(db.wo, wb); err != nil {
					logger.Errorf("write to db failed: %v", err)
					for i := range indexs {
						indexs[i] = 0
					}
				}
				wb.Clear()

				for i := range indexs {
					entrys[i].indexCh <- indexs[i]
				}
			}

			timer.Reset(time.Millisecond * time.Duration(db.maxWaitPerBatch))
		}
	}()
	return db, nil
}

type appendEntry struct {
	data              [][]byte
	producerID        uint64
	producerSeq       uint64
	LastPacificaIndex uint64
	indexCh           chan uint64
}

type DB struct {
	ctx       context.Context
	db        *grocksdb.DB
	cfHandles []*grocksdb.ColumnFamilyHandle

	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions

	minBytesPerBatch uint64
	maxWaitPerBatch  uint32

	appendCh    chan struct{}
	appendMu    sync.Mutex
	appendQueue []appendEntry
	appendBytes uint64
}

// ch返回0表示失败
func (db *DB) AppendMessages(msgs [][]byte, producerID *storage_info.ProducerID, LastPacificaIndex uint64) <-chan uint64 {
	appendIndexCh := make(chan uint64, 1)
	db.appendMu.Lock()
	db.appendQueue = append(db.appendQueue, appendEntry{
		data:              msgs,
		producerID:        producerID.GetID(),
		producerSeq:       producerID.GetSequence(),
		LastPacificaIndex: LastPacificaIndex,
		indexCh:           appendIndexCh,
	})
	for _, data := range msgs {
		db.appendBytes += uint64(len(data))
	}
	if db.appendBytes >= db.minBytesPerBatch {
		select {
		case db.appendCh <- struct{}{}:
		default:
		}
	}
	db.appendMu.Unlock()

	return appendIndexCh
}

func (db *DB) SetProducerID(producerID *storage_info.ProducerID, LastPacificaIndex uint64) error {
	ID := producerID.GetID()
	producerID.ID = nil
	now := time.Now().Unix()
	producerID.Timestamp = &now
	producerIDData, err := proto.Marshal(producerID)
	if err != nil {
		return err
	}
	err = db.db.Put(db.wo, append([]byte(MetaProducerIDPrefix), conv.Uint64ToBytes(ID)...), producerIDData)
	if err != nil {
		return err
	}
	return nil
}

// 考虑其它方法调用频率较少或只有一个消费者调用，可直接操作数据库
func (db *DB) GetMessage(beginIndex uint64, maxBytes uint32) ([]*storage_info.Record, error) {
	bytes := uint64(0)
	it := db.db.NewIteratorCF(db.ro, db.cfHandles[MsgCfIndex])
	defer it.Close()
	records := []*storage_info.Record{}
	for it.Seek(conv.Uint64ToBytes(beginIndex)); it.Valid() && bytes < uint64(maxBytes); it.Next() {
		key := it.Key()
		value := it.Value()
		record := &storage_info.Record{}
		err := proto.Unmarshal(value.Data(), record)
		if err != nil {
			logger.Errorf("proto unmarshal record failed: %v", err)
		}
		index := conv.BytesToUint64(key.Data())
		record.Index = &index
		records = append(records, record)
		bytes += uint64(len(value.Data()))
		key.Free()
		value.Free()
	}
	return records, nil
}

func (db *DB) GetProducerIDs() (map[uint64]uint64, error) {
	it := db.db.NewIteratorCF(db.ro, db.cfHandles[MetaCfIndex])
	defer it.Close()
	producerID2Seq := make(map[uint64]uint64)
	for it.Seek([]byte(MetaProducerIDPrefix)); it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()
		if strings.HasPrefix(string(key.Data()), MetaProducerIDPrefix) == false {
			key.Free()
			value.Free()
			break
		}
		producerID := &storage_info.ProducerID{}
		err := proto.Unmarshal(value.Data(), producerID)
		if err != nil {
			logger.Errorf("proto unmarshal producerID failed: %v", err)
		}
		ID := conv.BytesToUint64(key.Data()[len(MetaProducerIDPrefix):])
		Seq := producerID.GetSequence()
		producerID2Seq[ID] = Seq
		key.Free()
		value.Free()
	}
	return producerID2Seq, nil
}

func (db *DB) GetLastPacificaIndex() (uint64, error) {
	value, err := db.db.Get(db.ro, []byte(MetaLastPacificaIndex))
	defer value.Free()
	if err != nil {
		return 0, err
	}
	if len(value.Data()) == 0 {
		return 0, nil
	}
	return conv.BytesToUint64(value.Data()), nil
}

func (db *DB) CommitIndex(Index *storage_info.ConsumerCommitIndex, LastPacificaIndex uint64) error {
	groupID := Index.GetGroupID()
	Index.GroupID = nil
	now := time.Now().Unix()
	Index.Timestamp = &now
	value, err := proto.Marshal(Index)
	if err != nil {
		return err
	}

	err = db.db.Put(db.wo, append([]byte(MetaConsumedIndex), []byte(groupID)...), value)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) GetCommitedIndex(GroupID string) (uint64, error) {
	value, err := db.db.Get(db.ro, append([]byte(MetaConsumedIndex), []byte(GroupID)...))
	defer value.Free()
	if err != nil {
		return 0, err
	}
	Index := &storage_info.ConsumerCommitIndex{}
	if len(value.Data()) > 0 {
		err = proto.Unmarshal(value.Data(), Index)
		if err != nil {
			return 0, err
		}
	}
	return Index.GetCommitIndex(), nil
}

func (db *DB) close() {
	db.ro.Destroy()
	db.wo.Destroy()
	for _, cf := range db.cfHandles {
		cf.Destroy()
	}
	db.db.Close()
}
