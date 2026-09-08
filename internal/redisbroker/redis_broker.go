package redisbroker

import (
	"context"
	"log"
	"sync"

	"github.com/redis/go-redis/v9"
)

// RedisBroker implements types.IBroker using Redis Pub/Sub
type RedisBroker struct {
	client *redis.Client
	pubsub *redis.PubSub
	mu     sync.RWMutex
	ch     chan []byte
	closed bool
}

// Config cho Redis Broker
type Config struct {
	Addr     string // Redis address, default "localhost:6379"
	Password string
	DB       int
	Channel  string // Pub/Sub channel, default "ws-broadcast"
}

// DefaultConfig trả về config mặc định
func DefaultConfig() Config {
	return Config{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Channel:  "ws-broadcast",
	}
}

// NewRedisBroker tạo Redis broker mới
func NewRedisBroker(cfg Config) *RedisBroker {
	if cfg.Addr == "" {
		cfg.Addr = "localhost:6379"
	}
	if cfg.Channel == "" {
		cfg.Channel = "ws-broadcast"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Kiểm tra kết nối
	if err := client.Ping(context.Background()).Err(); err != nil {
		log.Printf("Redis connection warning: %v (continuing with LocalBroker fallback)", err)
		// Vẫn trả về broker nhưng sẽ log lỗi khi publish
	}

	return &RedisBroker{
		client: client,
		ch:     make(chan []byte, 256),
	}
}

// Publish gửi message lên Redis
func (r *RedisBroker) Publish(message []byte) error {
	if r.closed {
		return nil
	}
	if r.client == nil {
		return nil
	}
	return r.client.Publish(context.Background(), "ws-broadcast", message).Err()
}

// Subscribe đăng ký nhận message từ Redis
func (r *RedisBroker) Subscribe(ctx context.Context) (<-chan []byte, error) {
	if r.closed {
		return nil, nil
	}
	if r.client == nil {
		return nil, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pubsub != nil {
		return r.ch, nil
	}

	// Đăng ký subscribe
	r.pubsub = r.client.Subscribe(ctx, "ws-broadcast")

	// Goroutine nhận message từ Redis
	go func() {
		defer func() {
			if r.pubsub != nil {
				r.pubsub.Close()
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := r.pubsub.ReceiveMessage(ctx)
				if err != nil {
					if err != context.Canceled {
						log.Printf("Redis receive error: %v", err)
					}
					return
				}
				select {
				case r.ch <- []byte(msg.Payload):
				default:
					log.Println("Redis broker channel full, message dropped")
				}
			}
		}
	}()

	return r.ch, nil
}

// Close đóng kết nối Redis
func (r *RedisBroker) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true
	if r.pubsub != nil {
		r.pubsub.Close()
	}
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}
