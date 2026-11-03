package redis

import (
	"context"
	"fmt"

	"github.com/DVV-15324/witches-realtime/pkg/types"
	"github.com/redis/go-redis/v9"
)

type Subscriber struct {
	client  *redis.Client
	channel string
	logger  types.ILogger
}

func New(addr, password string, db int, channel string, logger types.ILogger) *Subscriber {
	return &Subscriber{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
		channel: channel,
		logger:  logger,
	}
}

func (s *Subscriber) Subscribe(ctx context.Context) (<-chan []byte, error) {
	pubsub := s.client.Subscribe(ctx, s.channel)
	if _, err := pubsub.Receive(ctx); err != nil {
		return nil, fmt.Errorf("redis subscribe: %w", err)
	}
	out := make(chan []byte, 1024)
	go func() {
		defer close(out)
		defer pubsub.Close()
		s.logger.Info(fmt.Sprintf("Redis subscribed to %s", s.channel))
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				s.logger.Info("Redis subscriber stopped")
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- []byte(msg.Payload):
				default:
					s.logger.Warn("redis channel full, dropping")
				}
			}
		}
	}()
	return out, nil
}

func (s *Subscriber) Close() error {
	return s.client.Close()
}
