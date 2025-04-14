package storage

import (
	"context"
	"errors"
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

func NewStorage(ops Options) (storage Storage, err error) {
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
	db.appendQueue = make([]appendTerm, 0, 20)
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
				return
			case <-db.appendCh:
			case <-timer.C:
			}

			getTerms := func() []appendTerm {
				bytes := uint64(0)
				terms := []appendTerm{}
				db.appendMu.Lock()
				defer db.appendMu.Unlock()
				for i := range db.appendQueue {
					terms = append(terms, db.appendQueue[i])
					for _, data := range db.appendQueue[i].data {
						bytes += uint64(len(data))
					}

					if bytes >= db.minBytesPerBatch {
						break
					}
				}
				db.appendBytes -= bytes
				db.appendQueue = db.appendQueue[len(terms):]
				if len(db.appendQueue) == 0 && cap(db.appendQueue) == 0 {
					db.appendQueue = make([]appendTerm, 0, 20)
				}
				return terms
			}
			for {
				terms := getTerms()
				if len(terms) == 0 {
					break
				}
				var indexs []uint64
				now := time.Now().Unix()
				for termIndex := range terms {
					wb.SetSavePoint()
					indexs = append(indexs, nextIndex)

					nextTerm := false
					for dataIndex := range terms[termIndex].data {
						record := &storage_info.Record{
							Data: terms[termIndex].data[dataIndex],
						}
						recordData, err := proto.Marshal(record)
						if err != nil {
							logger.Errorf("proto marshal record failed: %v", err)
							wb.RollbackToSavePoint()
							indexs[len(indexs)-1] = 0
							break
						}
						wb.PutCF(db.cfHandles[MsgCfIndex], conv.Uint64ToBytes(nextIndex), recordData)
						nextIndex++
					}
					if nextTerm {
						continue
					}

					producerID := &storage_info.ProducerID{
						Sequence:  &terms[termIndex].producerSeq,
						Timestamp: &now,
					}
					producerIDData, err := proto.Marshal(producerID)
					if err != nil {
						logger.Errorf("proto marshal producerID failed: %v", err)
						wb.RollbackToSavePoint()
						indexs[len(indexs)-1] = 0
						continue
					}
					wb.PutCF(db.cfHandles[MetaCfIndex], append([]byte(MetaProducerIDPrefix), conv.Uint64ToBytes(terms[termIndex].producerID)...), producerIDData)
				}
				wb.PutCF(db.cfHandles[MetaCfIndex], []byte(MetaLastPacificaIndex), conv.Uint64ToBytes(terms[len(terms)-1].LastPacificaIndex))
				if err := db.db.Write(db.wo, wb); err != nil {
					logger.Errorf("write to db failed: %v", err)
					for i := range indexs {
						indexs[i] = 0
					}
				}
				wb.Clear()

				for i := range indexs {
					terms[i].indexCh <- indexs[i]
				}
			}

			timer.Reset(time.Millisecond * time.Duration(db.maxWaitPerBatch))
		}
	}()
	return db, nil
}

type appendTerm struct {
	data              [][]byte
	producerID        uint64
	producerSeq       uint64
	LastPacificaIndex uint64
	indexCh           chan uint64
}
type DB struct {
	ctx       context.Context
	cancel    context.CancelFunc
	db        *grocksdb.DB
	cfHandles []*grocksdb.ColumnFamilyHandle

	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions

	minBytesPerBatch uint64
	maxWaitPerBatch  uint32

	appendCh    chan struct{}
	appendMu    sync.Mutex
	appendQueue []appendTerm
	appendBytes uint64
}

func (db *DB) AppendMessages(msgs *storage_info.ReplicativeData, producerID *storage_info.ProducerID, LastPacificaIndex uint64) (uint64, error) {
	appendIndexCh := make(chan uint64)
	db.appendMu.Lock()
	db.appendQueue = append(db.appendQueue, appendTerm{
		data:              msgs.Data,
		producerID:        producerID.GetID(),
		producerSeq:       producerID.GetSequence(),
		LastPacificaIndex: LastPacificaIndex,
		indexCh:           appendIndexCh,
	})
	for _, data := range msgs.Data {
		db.appendBytes += uint64(len(data))
	}
	if db.appendBytes >= db.minBytesPerBatch {
		select {
		case db.appendCh <- struct{}{}:
		default:
		}
	}
	db.appendMu.Unlock()

	appendIndex := <-appendIndexCh
	if appendIndex == 0 {
		return 0, errors.New("append index failed")
	} else {
		return appendIndex, nil
	}
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
		proto.Unmarshal(value.Data(), record)
		index := conv.BytesToUint64(key.Data())
		record.Index = &index
		records = append(records, record)
		bytes += uint64(len(value.Data()))
		key.Free()
		value.Free()
	}
	return records, nil
}

func (db *DB) GetProducerID(id uint64) (*storage_info.ProducerID, error) {
	value, err := db.db.Get(db.ro, append([]byte(MetaProducerIDPrefix), conv.Uint64ToBytes(id)...))
	defer value.Free()
	if err != nil {
		return nil, err
	}
	producerID := &storage_info.ProducerID{}
	err = proto.Unmarshal(value.Data(), producerID)
	if err != nil {
		return nil, err
	}
	producerID.ID = &id
	return producerID, nil
}

func (db *DB) GetLastPacificaIndex() (uint64, error) {
	value, err := db.db.Get(db.ro, []byte(MetaLastPacificaIndex))
	defer value.Free()
	if err != nil {
		return 0, err
	}
	return conv.BytesToUint64(value.Data()), nil
}

func (db *DB) CommitIndex(CommitIndex uint64) error {
	err := db.db.Put(db.wo, []byte(MetaConsumedIndex), conv.Uint64ToBytes(CommitIndex))
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) GetCommitedIndex() uint64 {
	value, err := db.db.Get(db.ro, []byte(MetaConsumedIndex))
	defer value.Free()
	if err != nil {
		return 0
	}
	if len(value.Data()) == 0 {
		return 0
	}
	return conv.BytesToUint64(value.Data())
}

func (db *DB) Close() {
	db.cancel()
	db.ro.Destroy()
	db.wo.Destroy()
	for _, cf := range db.cfHandles {
		cf.Destroy()
	}
	db.db.Close()
}
