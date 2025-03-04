package store

import (
	"context"
	"time"
)

type Message struct {
	Id           string
	DispatchTime time.Time
	Name         string
	Payload      string
}

type Store interface {
	CreateMessage(ctx context.Context, message Message, topic string) error
	ReadBatch(ctx context.Context, topic string, batchSize int32) ([]Message, error)
}
