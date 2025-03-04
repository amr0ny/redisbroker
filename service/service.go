/*
Package service provides the implementation of the QueueServiceServer interface,
which includes methods for adding messages to a store and consuming batches of messages
from the store. The package integrates with a store interface to manage message persistence
and retrieval.
*/
package service

import (
	"context"
	"github.com/amr0ny/redisbroker/protobuf/queue"
	"github.com/amr0ny/redisbroker/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// QueueService implements the QueueServiceServer interface.
type QueueService struct {
	queue.UnimplementedQueueServiceServer
	store store.Store
}

// AddMessage adds a new message to the store.
// It generates a new UUID for the message ID and sets the current time as the dispatch time.
// Returns an empty response or an error if the operation fails.
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

// Consume retrieves a batch of messages from the store based on the topic and batch size specified in the request.
// It converts the messages to the appropriate protobuf format and returns them in a BatchResponse.
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

// NewQueueService creates a new instance of QueueService with the provided store.
func NewQueueService(store store.Store) *QueueService {
	return &QueueService{store: store}
}

// RegisterServiceServer registers the QueueService with the given gRPC server.
func RegisterServiceServer(srv *grpc.Server, svc *QueueService) {
	queue.RegisterQueueServiceServer(srv, svc)
}
