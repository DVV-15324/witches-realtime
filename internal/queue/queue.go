// internal/queue/queue.go
package queue

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	mu       sync.RWMutex
	messages []Message
	file     *os.File
	writer   *bufio.Writer
	redis    *redis.Client
	channel  string
	ctx      context.Context
	cancel   context.CancelFunc
	handlers map[string]func(Message)
}

type Message struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Room      string          `json:"room"`
	Payload   json.RawMessage `json:"payload"`
	ClientID  string          `json:"client_id"`
	Timestamp time.Time       `json:"timestamp"`
	Retry     int             `json:"retry"`
	Status    string          `json:"status"` // pending, processing, done, failed
}

type Config struct {
	LogPath   string
	RedisAddr string
	RedisDB   int
	Channel   string
	MaxRetry  int
	BatchSize int
}

func NewQueue(cfg Config) (*Queue, error) {
	dir := filepath.Dir(cfg.LogPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	if info, err := os.Stat(cfg.LogPath); err == nil && info.IsDir() {
		log.Printf("[QUEUE] Warning: %s is a directory, removing it", cfg.LogPath)
		if err := os.RemoveAll(cfg.LogPath); err != nil {
			return nil, fmt.Errorf("path is a directory and cannot be removed: %w", err)
		}
	}

	file, err := os.OpenFile(cfg.LogPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	q := &Queue{
		messages: make([]Message, 0, 1000),
		file:     file,
		writer:   bufio.NewWriter(file),
		channel:  cfg.Channel,
		ctx:      ctx,
		cancel:   cancel,
		handlers: make(map[string]func(Message)),
	}

	if err := q.loadFromFile(); err != nil {
		log.Printf("[QUEUE] Load from file warning: %v", err)
	}

	if cfg.RedisAddr != "" {
		q.redis = redis.NewClient(&redis.Options{
			Addr: cfg.RedisAddr,
			DB:   cfg.RedisDB,
		})
		if err := q.redis.Ping(ctx).Err(); err != nil {
			log.Printf("[QUEUE] Redis warning: %v (continue without Redis)", err)
			q.redis = nil
		} else {
			log.Printf("[QUEUE] Connected to Redis: %s", cfg.RedisAddr)
			go q.subscribeRedis()
		}
	}

	go q.processor()

	return q, nil
}

func (q *Queue) Push(msg Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.messages = append(q.messages, msg)

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := q.writer.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := q.writer.Flush(); err != nil {
		return err
	}

	if q.redis != nil {
		go func() {
			if err := q.redis.Publish(q.ctx, q.channel, data).Err(); err != nil {
				log.Printf("[QUEUE] Redis publish error: %v", err)
			}
		}()
	}

	log.Printf("[QUEUE] Pushed: %s | type=%s | room=%s", msg.ID, msg.Type, msg.Room)
	return nil
}

func (q *Queue) Pop() *Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil
	}

	msg := q.messages[0]
	q.messages = q.messages[1:]
	return &msg
}

func (q *Queue) PopByType(msgType string) *Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i, msg := range q.messages {
		if msg.Type == msgType {
			q.messages = append(q.messages[:i], q.messages[i+1:]...)
			return &msg
		}
	}
	return nil
}

func (q *Queue) RegisterHandler(msgType string, handler func(Message)) {
	q.handlers[msgType] = handler
	log.Printf("[QUEUE] Registered handler for type: %s", msgType)
}

// UpdateStatus cập nhật status vào file
func (q *Queue) UpdateStatus(msg *Message, status string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	msg.Status = status

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := q.writer.Write(append(data, '\n')); err != nil {
		return err
	}
	return q.writer.Flush()
}

func (q *Queue) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.messages)
}

func (q *Queue) Close() error {
	q.cancel()
	if q.writer != nil {
		q.writer.Flush()
	}
	if q.file != nil {
		return q.file.Close()
	}
	if q.redis != nil {
		return q.redis.Close()
	}
	return nil
}

//  PROCESSOR

func (q *Queue) processor() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	log.Println("[QUEUE] Processor started")

	for {
		select {
		case <-q.ctx.Done():
			log.Println("[QUEUE] Processor stopped")
			return
		case <-ticker.C:
			q.processMessages()
		}
	}
}

func (q *Queue) processMessages() {
	for {
		msg := q.Pop()
		if msg == nil {
			break
		}

		handler, ok := q.handlers[msg.Type]
		if !ok {
			log.Printf("[QUEUE] No handler for type: %s (skip)", msg.Type)
			continue
		}

		// Update: processing
		if err := q.UpdateStatus(msg, "processing"); err != nil {
			log.Printf("[QUEUE] Failed to update status to processing: %v", err)
		}
		log.Printf("[QUEUE] Processing: %s | type=%s", msg.ID, msg.Type)

		handler(*msg)

		// Update: done
		if err := q.UpdateStatus(msg, "done"); err != nil {
			log.Printf("[QUEUE] Failed to update status to done: %v", err)
		}
		log.Printf("[QUEUE] Done: %s | type=%s", msg.ID, msg.Type)
	}
}

//  REDIS

func (q *Queue) subscribeRedis() {
	if q.redis == nil {
		return
	}

	pubsub := q.redis.Subscribe(q.ctx, q.channel)
	defer pubsub.Close()

	ch := pubsub.Channel()
	log.Printf("[QUEUE] Subscribed to Redis channel: %s", q.channel)

	for {
		select {
		case <-q.ctx.Done():
			log.Println("[QUEUE] Redis subscriber stopped")
			return
		case redisMsg := <-ch:
			var msg Message
			if err := json.Unmarshal([]byte(redisMsg.Payload), &msg); err != nil {
				log.Printf("[QUEUE] Redis parse error: %v", err)
				continue
			}
			q.Push(msg)
			log.Printf("[QUEUE] Received from Redis: %s", msg.ID)
		}
	}
}

//  LOAD FROM FILE

func (q *Queue) loadFromFile() error {
	if _, err := q.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(q.file)
	count := 0
	for scanner.Scan() {
		var msg Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		// Chỉ load message pending hoặc processing (chưa done)
		if msg.Status == "pending" || msg.Status == "processing" {
			q.messages = append(q.messages, msg)
			count++
		}
	}

	log.Printf("[QUEUE] Loaded %d pending messages from file", count)
	return scanner.Err()
}

//  HELPER FUNCTIONS

func NewMessage(msgType, room string, payload []byte, clientID string) Message {
	return Message{
		ID:        generateID(),
		Type:      msgType,
		Room:      room,
		Payload:   payload,
		ClientID:  clientID,
		Timestamp: time.Now(),
		Retry:     0,
		Status:    "pending",
	}
}

func generateID() string {
	return time.Now().Format("20060102150405") + "-" + randomString(6)
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
	}
	return string(b)
}
