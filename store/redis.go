package store

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

type RedisStore struct {
	inst *redis.Client
}

func NewRedisStore(ctx context.Context, addr, user, password string, poolSize int) (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr:       addr,
		DB:         0,
		PoolSize:   poolSize,
		Username:   user,
		Password:   password,
		PoolFIFO:   false,
		MaxConnAge: 5,
	})

	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("could not connect to redis: %v", err)
	}
	return &RedisStore{inst: client}, nil
}

func (rs *RedisStore) CreateMessage(ctx context.Context, message Message, topic string) error {
	_, err := rs.inst.XAdd(ctx, &redis.XAddArgs{
		Stream: topic,
		Values: map[string]interface{}{
			"id":            message.Id,
			"dispatch_time": message.DispatchTime.Format(time.RFC3339),
			"name":          message.Name,
			"payload":       message.Payload,
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to add message to stream: %v", err)
	}
	return err
}

func (rs *RedisStore) readAndDeleteBatchAtomic(ctx context.Context, topic string, batchSize int32) (interface{}, error) {
	script := `
        local result = redis.call('XREAD', 'BLOCK', 3000, 'COUNT', ARGV[1], 'STREAMS', KEYS[1])
        local messageIDs = {}
        for _, stream in ipairs(result) do
            for _, message in ipairs(stream['messages']) do
                table.insert(messageIDs, message['id'])
            end
        end
        if #messageIDs > 0 then
            redis.call('XDEL', KEYS[1], unpack(messageIDs))
        end
        return result
    `
	result, err := rs.inst.Eval(ctx, script, []string{topic}, batchSize).Result()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (rs *RedisStore) ReadBatch(ctx context.Context, topic string, batchSize int32) ([]Message, error) {
	result, err := rs.readAndDeleteBatchAtomic(ctx, topic, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read batch: %v", err)
	}
	resArr, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to serialize batch: %v", err)
	}
	batch := make([]Message, len(resArr))
	for i, item := range resArr {
		data, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to serialize message at index %d: unexpected type %T", i, item)
		}
		id, ok := data["id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to serialize id: %v", data["id"])
		}
		dispatchTime, ok := data["dispatch_time"].(time.Time)
		if !ok {
			return nil, fmt.Errorf("failed to serialize dispatch_time: %v", data["dispatch_time"])
		}

		name, ok := data["name"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to serialize name: %v", data["name"])
		}
		payload, ok := data["payload"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to serialize payload: %v", data["payload"])
		}
		batch[i] = Message{
			Id:           id,
			DispatchTime: dispatchTime,
			Name:         name,
			Payload:      payload,
		}
	}
	return batch, nil
}
