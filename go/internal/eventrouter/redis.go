package eventrouter

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type RedisStream struct {
	client redis.UniversalClient
}

func NewRedisStream(client redis.UniversalClient) *RedisStream {
	return &RedisStream{client: client}
}

func (s *RedisStream) Publish(ctx context.Context, event StreamEvent) error {
	if s == nil || s.client == nil {
		return nil
	}
	payload, err := Encode(event)
	if err != nil {
		return err
	}
	return s.client.Publish(ctx, "sse:"+event.StreamID, payload).Err()
}

func (s *RedisStream) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}
