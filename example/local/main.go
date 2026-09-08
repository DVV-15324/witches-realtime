package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"realtime/internal/config"
	"realtime/internal/hub"
	bro "realtime/internal/hub/broardcast/broker"
	"realtime/internal/hub/helper"
	"realtime/internal/model"
	"realtime/internal/queue"
	"realtime/internal/types"
	"realtime/pkg/adapter"
	"realtime/pkg/uid"
	"time"
)

func main() {

	// 1. INIT COMPONENTS
	uid.Bits = 26
	idGen := adapter.NewUIDAdapter(1)
	logger := &SimpleLogger{}
	q, err := queue.NewQueue(queue.Config{
		LogPath:   "./data/queue/queue.log",
		RedisAddr: "", // GetRoomID Không dùng Redis, để trống
		RedisDB:   0,
		Channel:   "ws-broadcast",
	})
	if err != nil {
		log.Fatal("Failed to init queue:", err)
	}
	defer q.Close()

	// Đăng ký handlers cho queue
	q.RegisterHandler("local", func(msg queue.Message) {
		log.Printf("[QUEUE-HANDLER] local: room=%s, payload=%s",
			msg.Room, string(msg.Payload))
		// Có thể lưu DB, analytics, v.v...
	})

	q.RegisterHandler("broker", func(msg queue.Message) {
		log.Printf("[QUEUE-HANDLER] broker: payload=%s", string(msg.Payload))
	})
	// 2. CONFIG
	cfg := config.Config{
		Logger:       logger,
		UID:          idGen,
		Broker:       nil, // Local mode: không cần Redis
		AuthFunc:     nil,
		ShutdownCtx:  context.Background(),
		WSPath:       "/ws",
		PingInterval: 30 * time.Second,

		// Enable persist để ghi file log
		EnablePersist: true,
		PersistPath:   "./data/messages/",
	}
	// 3. CREATE SERVER
	rt := hub.NewServer(cfg)
	hub.SetQueue(q)
	// 4. SETUP CALLBACKS
	// LOCAL EVENT (message từ client)
	rt.Hub.OnLocalMessage = func(client types.IClient, msg []byte) {
		log.Printf("[LOCAL] [%s] message: %s", client.GetID(), string(msg))
		// Parse message để lấy action
		var req struct {
			Action  string          `json:"action"`
			Room    string          `json:"room,omitempty"`
			Payload json.RawMessage `json:"payload,omitempty"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			log.Printf("Invalid message: %v", err)
			return
		}
		// Nếu là action chat
		if req.Action == "chat" {
			var chatMsg model.ChatMessage
			if err := json.Unmarshal(req.Payload, &chatMsg); err != nil {
				log.Printf("Invalid chat payload: %v", err)
				return
			}
			// GetRoomID Broadcast đến room của client
			if client.GetRoomID() != "" {
				// Gửi đến room cụ thể (local + broker)
				bro.BroadcastToRoomAndBroker(rt.Hub, client.GetRoomID(), msg)
			} else {
				// Nếu chưa có room, broadcast global
				// GetRoomID Dùng BroadcastToAll thay vì gọi unexported method
				helper.BroadcastToAll(rt.Hub, model.BaseMessage{
					Type: "chat",
					Data: chatMsg,
				})
			}
		}
	}
	// BROKER EVENT (message từ Redis) - KHÔNG DÙNG local
	rt.Hub.OnBrokerMessage = func(msg []byte) {
		log.Printf("[BROKER] received: %s (local mode - no Redis)", string(msg))
	}
	// CONNECT
	rt.Hub.OnConnect = func(client types.IClient) {
		log.Printf("[%s] connected", client.GetID())
		// Gửi tin nhắn chào mừng riêng
		client.SendMessage(model.BaseMessage{
			Type: "system",
			Data: map[string]string{"message": fmt.Sprintf("Welcome %s! Join a room to chat.", client.GetID())},
		})
		// Thông báo cho mọi người (local)
		helper.BroadcastToAll(rt.Hub, model.BaseMessage{
			Type: "system",
			Data: map[string]string{"message": fmt.Sprintf("%s joined the chat", client.GetID())},
		})
	}

	// DISCONNECT
	rt.Hub.OnDisconnect = func(client types.IClient) {
		log.Printf("[%s] disconnected", client.GetID())

		// Thông báo cho mọi người (local)
		helper.BroadcastToAll(rt.Hub, model.BaseMessage{
			Type: "system",
			Data: map[string]string{"message": fmt.Sprintf("%s left the chat", client.GetID())},
		})
	}

	// 5. REST API
	http.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Room    string `json:"room"`
			Message string `json:"message"`
			Sender  string `json:"sender"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Room == "" {
			req.Room = "general"
		}
		msg := model.ChatMessage{
			Username: req.Sender,
			Message:  req.Message,
			Time:     time.Now().Format("15:04:05"),
		}
		data, _ := msg.ToJSON()

		// GetRoomID Broadcast đến room (local + broker nếu có)
		bro.BroadcastToRoomAndBroker(rt.Hub, req.Room, data)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// 6. STATIC FILES & WEBSOCKET

	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/ws", rt.Handler())

	// 7. START SERVER

	log.Println("Server starting on http://localhost:8080")
	log.Println("WebSocket endpoint: ws://localhost:8080/ws")
	log.Println("REST API: POST /send with JSON {room, message, sender}")
	log.Println("Log file: ./data/messages/messages-YYYY-MM-DD.log")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("Server error:", err)
	}
}

// SIMPLE LOGGER

type SimpleLogger struct{}

func (l *SimpleLogger) Info(msg string, args ...interface{}) {
	log.Printf("[INFO] "+msg, args...)
}
func (l *SimpleLogger) Error(msg string, args ...interface{}) {
	log.Printf("[ERROR] "+msg, args...)
}
func (l *SimpleLogger) Debug(msg string, args ...interface{}) {
	log.Printf("[DEBUG] "+msg, args...)
}
func (l *SimpleLogger) Warn(msg string, args ...interface{}) {
	log.Printf("[WARN] "+msg, args...)
}
