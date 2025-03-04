package service

import (
	"context"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"redisbroker/protobuf/queue"
	"redisbroker/store"
	"time"
)

type QueueService struct {
	queue.UnimplementedQueueServiceServer
	store store.Store
}

func (qs *QueueService) AddMessage(ctx context.Context, req *queue.MessageRequest) (*emptypb.Empty, error) {
	err := qs.store.CreateMessage(ctx, store.Message{
		Id:           uuid.New().String(),
		Name:         req.Name,
		Payload:      req.Payload,
		DispatchTime: time.Now(),
	}, req.Topic)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "method AddMessage ended up with an error: %v", err)
	}
	return nil, nil
}

func (qs *QueueService) Consume(ctx context.Context, req *queue.BatchRequest) (*queue.BatchResponse, error) {
	batch, err := qs.store.ReadBatch(ctx, req.Topic, req.BatchSize)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "method Consume ended up with an error: %v", err)
	}
	var res *queue.BatchResponse
	res.Messages = make([]*queue.MessageBatch, len(batch))
	for i, message := range batch {
		dispatchTime := timestamppb.New(message.DispatchTime)
		res.Messages[i] = &queue.MessageBatch{
			Name:    message.Name,
			Payload: message.Payload,
			MsgMetadata: &queue.MessageMetadata{
				Id:           message.Id,
				DispatchTime: dispatchTime,
			},
		}
	}
	return res, err
}

func NewQueueService(store store.Store) *QueueService {
	return &QueueService{store: store}
}

func RegisterServiceServer(srv *grpc.Server, svc *QueueService) {
	queue.RegisterQueueServiceServer(srv, svc)
}
