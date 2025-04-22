package broker

import (
	"context"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	opration "github.com/yeyeye2333/PacificaMQ/api/broker_opration"
	rpc "github.com/yeyeye2333/PacificaMQ/api/broker_rpc"
	"github.com/yeyeye2333/PacificaMQ/api/storage_info"
	"github.com/yeyeye2333/PacificaMQ/internal/logger"
	"github.com/yeyeye2333/PacificaMQ/internal/storage"
	"github.com/yeyeye2333/PacificaMQ/pacifica"
)

// todo:提交lastPacificaIndex,但需保证不丢失(参考kiwi)
func NewPartition(ctx context.Context, opts *partitionOptions) (*partition, error) {
	p := &partition{
		ctx:         ctx,
		partitionID: opts.PartitionID,
		maxTimeOut:  time.Duration(opts.MaxTimeOut) * time.Millisecond,
		commitMap:   make(map[commitKey]chan *applyRet),
	}

	storage, err := storage.NewStorage(ctx, opts.StorageOpts)
	if err != nil {
		return nil, err
	}
	p.storage = storage
	p.producerMap, err = p.storage.GetProducerIDs()
	if err != nil {
		return nil, err
	}
	for id := range p.producerMap {
		if id > p.nextProducerID {
			p.nextProducerID = id
		}
	}
	p.nextProducerID++

	p.pacifica, err = pacifica.NewNode(ctx, opts.PacificaOpts)
	if err != nil {
		return nil, err
	}
	p.applyCh = p.pacifica.ApplyCh()

	return p, nil
}

type commitKey struct {
	index   uint64
	version uint64
}

type applyRet struct {
	ret               rpc.RetCode
	IndexOrProducerID uint64
}

type producerValue struct {
	sequence uint64
	index    uint64
}

type partition struct {
	ctx context.Context

	partitionID int32
	maxTimeOut  time.Duration
	pacifica    *pacifica.Node
	storage     storage.Storage
	applyCh     <-chan *pacifica.ApplyMsg

	mu             sync.Mutex
	commitMap      map[commitKey]chan *applyRet
	producerMap    map[uint64]uint64
	nextProducerID uint64
}

func (p *partition) close() {

}

func (p *partition) apply() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case apply := <-p.applyCh:
			func() {
				p.mu.Lock()
				defer p.mu.Unlock()
				for _, entry := range apply.Entries {
					key := commitKey{
						index:   entry.GetIndex(),
						version: entry.GetVersion(),
					}
					ch, ok := p.commitMap[key]

					op := &opration.BrokerOperation{}
					err := proto.Unmarshal(entry.GetData(), op)
					if err != nil {
						logger.Error("unmarshal BrokerOperation failed", err)
						if ok {
							ch <- &applyRet{
								ret: rpc.RetCode_ErrOther,
							}
						}
						continue
					}

					switch v := op.GetOperation().(type) {
					case *opration.BrokerOperation_PushMsg:
						if seq, ok := p.producerMap[v.PushMsg.GetProducerID()]; ok && seq >= v.PushMsg.GetSequenceNumber() {
							ch <- &applyRet{
								ret:               rpc.RetCode_SUCCESS,
								IndexOrProducerID: v.PushMsg.GetProducerID(),
							}
							continue
						}

						id := v.PushMsg.GetProducerID()
						seq := v.PushMsg.GetSequenceNumber()
						producerID := &storage_info.ProducerID{
							ID:       &id,
							Sequence: &seq,
						}
						indexCh := p.storage.AppendMessages(v.PushMsg.GetDatas(), producerID, entry.GetIndex())
						go func() {
							Index := <-indexCh
							p.mu.Lock()
							defer p.mu.Unlock()
							ch, ok := p.commitMap[key]
							if Index == 0 {
								logger.Error("append message failed")
								if ok {
									ch <- &applyRet{
										ret: rpc.RetCode_ErrOther,
									}
								}
							} else {
								if seq, ok := p.producerMap[v.PushMsg.GetProducerID()]; !ok || seq < v.PushMsg.GetSequenceNumber() {
									p.producerMap[v.PushMsg.GetProducerID()] = v.PushMsg.GetSequenceNumber()
								}
								if ok {
									ch <- &applyRet{
										ret:               rpc.RetCode_SUCCESS,
										IndexOrProducerID: Index,
									}
								}
							}
						}()
					case *opration.BrokerOperation_CommitIndex:
						go func() {
							groupID := v.CommitIndex.GetConsumerGroup()
							index := v.CommitIndex.GetCommitIndex()
							commitIndex := &storage_info.ConsumerCommitIndex{
								GroupID:     &groupID,
								CommitIndex: &index,
							}
							err := p.storage.CommitIndex(commitIndex)
							p.mu.Lock()
							defer p.mu.Unlock()
							ch, ok := p.commitMap[key]
							if err != nil {
								logger.Error("commit index failed", err)
								if ok {
									ch <- &applyRet{
										ret: rpc.RetCode_ErrOther,
									}
								}
							} else {
								if ok {
									ch <- &applyRet{
										ret: rpc.RetCode_SUCCESS,
									}
								}
							}
						}()
					case *opration.BrokerOperation_InitProducerID:
						id := p.nextProducerID
						p.nextProducerID++
						go func() {
							err := p.storage.SetProducerID(&storage_info.ProducerID{
								ID:       &id,
								Sequence: new(uint64),
							})
							p.mu.Lock()
							defer p.mu.Unlock()
							ch, ok := p.commitMap[key]
							if err != nil {
								logger.Error("set producer id failed", err)
								if ok {
									ch <- &applyRet{
										ret: rpc.RetCode_ErrOther,
									}
								}
							} else {
								if ok {
									ch <- &applyRet{
										ret:               rpc.RetCode_SUCCESS,
										IndexOrProducerID: id,
									}
								}
							}
						}()
					}
				}
			}()
		}
	}

}

func (p *partition) startOp(op *opration.BrokerOperation) interface{} {
	ret := rpc.RetCode_SUCCESS
	var IndexOrProducerID uint64
	var applyIndex, version uint64
	applyData, err := proto.Marshal(op)
	if err != nil {
		ret = rpc.RetCode_ErrOther
	} else {
		applyIndex, version, err = p.pacifica.Apply(applyData)
		if err != nil {
			if err == pacifica.ErrNotLeader {
				ret = rpc.RetCode_NotLeader
			} else {
				logger.Error("apply failed", err)
				ret = rpc.RetCode_ErrOther
			}
		}
	}

	ch := make(chan *applyRet, 1)
	p.mu.Lock()
	key := commitKey{
		index:   applyIndex,
		version: version,
	}
	p.commitMap[key] = ch
	p.mu.Unlock()
	select {
	case <-p.ctx.Done():
		ret = rpc.RetCode_NotLeader
	case applyRet := <-ch:
		ret = applyRet.ret
		IndexOrProducerID = applyRet.IndexOrProducerID
	case <-time.After(p.maxTimeOut):
		ret = rpc.RetCode_TimeOut
	}

	switch op.GetOperation().(type) {
	case *opration.BrokerOperation_PushMsg:
		response := &rpc.PushMessagesResponse_TopicResponses_PartitionResponses{
			PartitionID: &p.partitionID,
			Ret:         &ret,
		}
		if ret == rpc.RetCode_SUCCESS {
			response.StartIndex = &IndexOrProducerID
		}
		return response
	case *opration.BrokerOperation_CommitIndex:
		response := &rpc.CommitIndexResponse{
			Ret: &ret,
		}
		return response
	case *opration.BrokerOperation_InitProducerID:
		response := &rpc.InitProducerIDResponse{
			Ret: &ret,
		}
		if ret == rpc.RetCode_SUCCESS {
			response.ProducerID = &IndexOrProducerID
		}
		return response
	default:
		logger.Error("unknown operation type")
		return nil
	}
}

func (p *partition) pushMessage(request *rpc.PushMessagesRequest_Topic_Partition) *rpc.PushMessagesResponse_TopicResponses_PartitionResponses {
	op := &opration.BrokerOperation{
		Operation: &opration.BrokerOperation_PushMsg{
			PushMsg: &opration.PushMessage{
				Datas:          request.Datas,
				ProducerID:     request.ProducerID,
				SequenceNumber: request.SequenceNumber,
			},
		},
	}
	return p.startOp(op).(*rpc.PushMessagesResponse_TopicResponses_PartitionResponses)
}

func (p *partition) pullMessage(request *rpc.PullMessagesRequest_Topic_Partition) *rpc.PullMessagesResponse_TopicResponses_PartitionResponses {
	ret := rpc.RetCode_SUCCESS
	if p.pacifica.IsLeader() == 0 {
		ret = rpc.RetCode_NotLeader
		return &rpc.PullMessagesResponse_TopicResponses_PartitionResponses{
			PartitionID: &p.partitionID,
			Ret:         &ret,
		}
	}

	records, err := p.storage.GetMessage(request.GetFetchIndex(), request.GetMaxBytes())
	if err != nil {
		logger.Error("get message failed", err)
		ret = rpc.RetCode_ErrOther
		return &rpc.PullMessagesResponse_TopicResponses_PartitionResponses{
			PartitionID: &p.partitionID,
			Ret:         &ret,
		}
	}

	var datas [][]byte
	for _, record := range records {
		datas = append(datas, record.GetData())
	}
	return &rpc.PullMessagesResponse_TopicResponses_PartitionResponses{
		PartitionID: &p.partitionID,
		Ret:         &ret,
		Datas:       datas,
	}
}

func (p *partition) commitIndex(request *rpc.CommitIndexRequest) *rpc.CommitIndexResponse {
	op := &opration.BrokerOperation{
		Operation: &opration.BrokerOperation_CommitIndex{
			CommitIndex: &opration.CommitIndex{
				ConsumerGroup: request.ConsumerGroup,
				CommitIndex:   request.CommitIndex,
			},
		},
	}
	return p.startOp(op).(*rpc.CommitIndexResponse)
}

func (p *partition) getCommitIndex(request *rpc.GetCommitIndexRequest) *rpc.GetCommitIndexResponse {
	ret := rpc.RetCode_SUCCESS
	if p.pacifica.IsLeader() == 0 {
		ret = rpc.RetCode_NotLeader
		return &rpc.GetCommitIndexResponse{
			Ret: &ret,
		}
	}

	commitIndex, err := p.storage.GetCommitedIndex(request.GetConsumerGroup())
	if err != nil {
		logger.Error("get commit index failed", err)
		ret = rpc.RetCode_ErrOther
		return &rpc.GetCommitIndexResponse{
			Ret: &ret,
		}
	}
	return &rpc.GetCommitIndexResponse{
		Ret:         &ret,
		CommitIndex: &commitIndex,
	}
}

func (p *partition) initProducerID(request *rpc.InitProducerIDRequest) *rpc.InitProducerIDResponse {
	op := &opration.BrokerOperation{
		Operation: &opration.BrokerOperation_InitProducerID{
			InitProducerID: &opration.InitProducerID{},
		},
	}
	return p.startOp(op).(*rpc.InitProducerIDResponse)
}
