package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"realtime/internal/config"
	"realtime/internal/hub"
	"realtime/internal/hub/action"
	bro "realtime/internal/hub/broardcast/broker"
	"realtime/internal/model"
	"realtime/internal/queue"
	"realtime/internal/redisbroker"
	"realtime/internal/types"
	"realtime/internal/worker"
	"realtime/pkg/adapter"
	"realtime/pkg/uid"
	"time"
)

func main() {
	//
	// 1. INIT COMPONENTS
	//
	uid.Bits = 26
	idGen := adapter.NewUIDAdapter(1)
	logger := &SimpleLogger{}

	//
	// 2. INIT REDIS BROKER
	//
	broker := redisbroker.NewRedisBroker(redisbroker.Config{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Channel:  "ws-broadcast",
	})
	defer broker.Close()

	//
	// 3. INIT QUEUE (chỉ để xử lý bất đồng bộ, KHÔNG broadcast)
	//
	q, err := queue.NewQueue(queue.Config{
		LogPath:   "./data/queue/queue.log",
		RedisAddr: "", // Queue không dùng Redis riêng
		RedisDB:   0,
		Channel:   "ws-broadcast",
	})
	if err != nil {
		log.Println("Queue init warning:", err)
	} else {
		defer q.Close()

		// Đăng ký handler để queue xử lý message (chỉ làm tác vụ phụ)
		q.RegisterHandler("local", func(msg queue.Message) {
			log.Printf("[QUEUE-HANDLER] local: room=%s, payload=%s", msg.RoomID, string(msg.Payload))
			// Ở đây có thể lưu DB, gửi analytics, notification...
		})

		q.RegisterHandler("broker", func(msg queue.Message) {
			log.Printf("[QUEUE-HANDLER] broker: room=%s, payload=%s", msg.RoomID, string(msg.Payload))
		})

		hub.SetQueue(q)
	}

	//
	// 4. CONFIG SERVER
	//
	cfg := config.Config{
		Logger:        logger,
		UID:           idGen,
		Broker:        broker,
		AuthFunc:      nil,
		ShutdownCtx:   context.Background(),
		WSPath:        "/ws",
		PingInterval:  30 * time.Second,
		EnablePersist: true,
		PersistPath:   "./data/messages/",
		RedisAddr:     "localhost:6379",
		RedisPassword: "",
		RedisDB:       0,
		RedisChannel:  "ws-broadcast",
	}

	//
	// 5. CREATE SERVER
	//
	rt := hub.NewServer(cfg)

	//
	// 6. KHỞI TẠO QUEUE WORKER (chỉ xử lý tác vụ phụ, KHÔNG broadcast)
	//
	if q != nil {
		// Handler cho worker: chỉ log và làm tác vụ phụ
		queueHandler := func(msg *queue.Message) {
			log.Printf("[WORKER] Processing message %s, type=%s, room=%s", msg.ID, msg.Type, msg.RoomID)

			// Ví dụ: nếu là chat message, có thể lưu vào DB
			switch msg.Type {
			case "chat":
				var chatMsg model.ChatMessage
				if err := json.Unmarshal(msg.Payload, &chatMsg); err != nil {
					log.Printf("[WORKER] Invalid chat payload: %v", err)
					return
				}
				// Giả sử lưu vào DB (placeholder)
				log.Printf("[WORKER] Saving chat to DB: %+v", chatMsg)

			case "system":
				log.Printf("[WORKER] System message: %s", string(msg.Payload))

			default:
				log.Printf("[WORKER] Unknown message type: %s", msg.Type)
			}
		}

		qw := worker.NewQueueWorker(q, queueHandler, 5, 200*time.Millisecond)
		qw.Start()
		defer qw.Stop()
	}

	//
	// 7. SETUP CALLBACKS
	//

	// LOCAL EVENT (message từ client) – KHÔNG BROADCAST Ở ĐÂY
	rt.Hub.OnLocalMessage = func(client types.IClient, msg []byte) {
		log.Printf("[LOCAL] [%s] message: %s", client.GetID(), string(msg))

		// Parse để biết action (nếu cần)
		var req struct {
			Action  string          `json:"action"`
			Room    string          `json:"room,omitempty"`
			Payload json.RawMessage `json:"payload,omitempty"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			log.Printf("Invalid message: %v", err)
			return
		}

		// Xử lý business logic (ví dụ: kiểm tra quyền, lưu DB)
		if req.Action == "chat" {
			var chatMsg model.ChatMessage
			if err := json.Unmarshal(req.Payload, &chatMsg); err != nil {
				log.Printf("Invalid chat payload: %v", err)
				return
			}
			// Lưu DB, gửi notification, v.v...
			log.Printf("[BUSINESS] Chat from %s in room %s: %s",
				chatMsg.Username, client.GetRoomID(), chatMsg.Message)
		}
		// KHÔNG GỌI broadcast ở đây vì hub đã làm rồi
	}

	// BROKER EVENT (message từ Redis) – KHÔNG BROADCAST
	rt.Hub.OnBrokerMessage = func(msg []byte) {
		log.Printf("[BROKER] received: %s", string(msg))
		// Có thể xử lý thêm, nhưng không broadcast vì hub đã xử lý
	}

	// CONNECT
	rt.Hub.OnConnect = func(client types.IClient) {
		log.Printf("[%s] connected", client.GetID())

		// Gửi tin nhắn riêng
		client.SendMessage(model.BaseMessage{
			Type: "system",
			Data: map[string]string{"message": fmt.Sprintf("Welcome %s! Join a room to chat.", client.GetID())},
		})

		// Broadcast thông báo join (dùng helper có sẵn)
		bro.BroadcastToAllAndBroker(rt.Hub, model.BaseMessage{
			Type: "system",
			Data: map[string]string{"message": fmt.Sprintf("%s joined the chat", client.GetID())},
		}, q)
	}

	// DISCONNECT
	rt.Hub.OnDisconnect = func(client types.IClient) {
		log.Printf("[%s] disconnected", client.GetID())

		// Rời khỏi room hiện tại
		if room := client.GetRoomID(); room != "" {
			action.LeaveRoom(rt.Hub, client.GetID(), room)
		}

		// Broadcast thông báo leave
		bro.BroadcastToAllAndBroker(rt.Hub, model.BaseMessage{
			Type: "system",
			Data: map[string]string{"message": fmt.Sprintf("%s left the chat", client.GetID())},
		}, q)
	}

	//
	// 8. REST API (gửi tin nhắn từ HTTP) – vẫn dùng helper broadcast
	//
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

		// Broadcast trực tiếp (và cũng đẩy vào queue nếu helper hỗ trợ)
		bro.BroadcastToRoomAndBroker(rt.Hub, req.Room, data)
		w.Write([]byte(`{"status":"ok"}`))
	})

	//
	// 9. STATIC FILES & WEBSOCKET
	//
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/ws", rt.Handler())

	//
	// 10. START SERVER
	//
	log.Println("Server starting on http://localhost:8080")
	log.Println("WebSocket endpoint: ws://localhost:8080/ws")
	log.Println("REST API: POST /send with JSON {room, message, sender}")
	log.Println("Log file: ./data/messages/messages-YYYY-MM-DD.log")
	log.Println("Queue file: ./data/queue/queue.log")
	log.Println("Redis: connected to localhost:6379")

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
