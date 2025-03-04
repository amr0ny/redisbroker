/*
Package store provides an interface and data structures for managing messages in a storage system.
*/

package store

import (
	"context"
	"time"
)

// Message is a universal unit being sent over gRPC by Redis Broker
type Message struct {
	Id           string    // Id is UUID unique identifier automatically assigned by broker
	DispatchTime time.Time // DispatchTime is assigned by broker
	Name         string    // User can provide a message its Name
	Payload      string    // Payload contains the most informative part of the message
}

// Store is an interface describing a type that can manage messages.
type Store interface {

	// CreateMessage creates a new message in the store.
	// Parameters:
	// - ctx: context for managing request lifetime.
	// - message: the Message object to be created.
	// - topic: the topic under which the message will be stored.
	// Returns an error if the operation fails.
	CreateMessage(ctx context.Context, message Message, topic string) error

	// ReadBatch reads a batch of messages from the store.
	// Parameters:
	// - ctx: context for managing request lifetime.
	// - topic: the topic from which messages will be read.
	// - batchSize: the number of messages to read in one batch.
	// Returns a slice of Message objects and an error if the operation fails.
	ReadBatch(ctx context.Context, topic string, batchSize int32) ([]Message, error)
}
