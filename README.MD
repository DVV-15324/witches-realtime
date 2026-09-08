



## Khi nào cần Redis Broker thật?

| Tình huống | Cần Redis? |
| :--- | :--- |
| Chạy 1 instance duy nhất | ❌ Không cần (LocalBroker đủ) |
| Chạy nhiều instance (scale horizontal) | ✅ Cần Redis để sync message |
| Chạy microservices riêng biệt | ✅ Cần Redis để giao tiếp |
| Production có nhiều user | ✅ Cần Redis để scale |

## Tóm tắt kiến trúc hiện tại
```text
Client
  │
  ▼
ReadPump
  │
  ▼
HandleLocalMessage
  │
  ├── broadcastToLocalClients()
  │
  └── Broker.Publish()
             │
             ▼
          Redis
             │
             ▼
      brokerBroadcast
             │
             ▼
    handleBrokerEvent()
             │
             ▼
    broadcastToLocalClients()
             │
             ▼
        client.Send
             │
             ▼
         WritePump

```
Dựa trên file kiến trúc của bạn, tôi sẽ vẽ lại sơ đồ tổng thể và chi tiết hơn:

## SƠ ĐỒ TỔNG THỂ

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              WEBSOCKET SERVER (Instance 1)                          │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                              CLIENT A                                        │   │
│  │                        (WebSocket Connection)                               │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                               │
│                                     ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ 1. ReadPump                                                                 │   │
│  │    - conn.ReadMessage() → nhận raw message                                  │   │
│  │    - Parse action: {"action":"chat", "payload": {...}}                      │   │
│  │    - switch action:                                                         │   │
│  │      • "join" → HandleAction() → JoinRoom()                                 │   │
│  │      • "leave" → HandleAction() → LeaveRoom()                               │   │
│  │      • default → Hub.handleLocalMessage()                                  │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                               │
│                                     ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ 2. Hub.handleLocalMessage()                                                 │   │
│  │    ┌─────────────────────────────────────────────────────────────────┐     │   │
│  │    │ a. OnLocalMessage(client, message) → BUSINESS LOGIC             │     │   │
│  │    │    - Validate message                                            │     │   │
│  │    │    - Save to database                                            │     │   │
│  │    │    - Transform/Enrich data                                       │     │   │
│  │    └─────────────────────────────────────────────────────────────────┘     │   │
│  │                                    │                                        │   │
│  │    ┌───────────────────────────────┴────────────────────────────────┐     │   │
│  │    │                                                                │     │   │
│  │    ▼                                                                ▼     │   │
│  │  ┌─────────────────────────────────┐    ┌──────────────────────────┐     │   │
│  │  │ b. broadcastToLocalClients()    │    │ c. Queue.Push()          │     │   │
│  │  │    - Gửi đến clients khác       │    │    - Lưu vào Queue        │     │   │
│  │  │      trên cùng instance         │    │    - Async processing    │     │   │
│  │  └─────────────────────────────────┘    └──────────────────────────┘     │   │
│  │                                    │                                      │   │
│  │                                    ▼                                      │   │
│  │                         ┌──────────────────────────┐                     │   │
│  │                         │ d. Broker.Publish()      │                     │   │
│  │                         │    - Gửi lên Redis       │                     │   │
│  │                         └──────────────────────────┘                     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                               │
│                                     ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ 3. Client.SendBytes()                                                       │   │
│  │    - Non-blocking send: select { case c.Send <- data }                     │   │
│  │    - Drop if buffer full                                                    │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                               │
│                                     ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ 4. WritePump                                                                │   │
│  │    - conn.WriteMessage(TextMessage, message)                                │   │
│  │    - Ping ticker: 30s                                                       │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                               │
│                                     ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                              CLIENT A                                        │   │
│  │                        (Receive message)                                    │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     │ Redis Pub/Sub
                                     ▼

┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              REDIS (Message Broker)                                  │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ Channel: "ws-broadcast"                                                      │   │
│  │                                                                              │   │
│  │  ┌────────────────────────────────────────────────────────────────────┐     │   │
│  │  │ Subscriber 1 (Instance 1)    Subscriber 2 (Instance 2)             │     │   │
│  │  └────────────────────────────────────────────────────────────────────┘     │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     │
                                     ▼

┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              WEBSOCKET SERVER (Instance 2)                          │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ 5. Broker.Subscribe() (Goroutine)                                            │   │
│  │    - r.pubsub = r.client.Subscribe(ctx, "ws-broadcast")                     │   │
│  │    - Loop: receive message → send to channel r.ch                           │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                               │
│                                     ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ 6. Hub.Run()                                                                 │   │
│  │    - brokerCh, _ := h.broker.Subscribe(h.ctx)                               │   │
│  │    - for msg := range brokerCh → h.brokerBroadcast <- msg                   │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                               │
│                                     ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ 7. Hub.handleBrokerEvent()                                                  │   │
│  │    ┌─────────────────────────────────────────────────────────────────┐     │   │
│  │    │ a. OnBrokerMessage(msg) → CALLBACK                              │     │   │
│  │    │    - Log message                                                │     │   │
│  │    └─────────────────────────────────────────────────────────────────┘     │   │
│  │                                    │                                        │   │
│  │    ┌───────────────────────────────┴────────────────────────────────┐     │   │
│  │    │                                                                │     │   │
│  │    ▼                                                                ▼     │   │
│  │  ┌─────────────────────────────────┐    ┌──────────────────────────┐     │   │
│  │  │ b. Check Room                   │    │ c. broadcastToLocal      │     │   │
│  │  │    if wrapped.Room != ""        │    │    Clients()             │     │   │
│  │  │    → BroadcastToRoomLocal()    │    │    - Gửi đến tất cả      │     │   │
│  │  └─────────────────────────────────┘    │      clients local      │     │   │
│  │                                         └──────────────────────────┘     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                               │
│                                     ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ 8. Client.SendBytes() → WritePump → CLIENT B                                │   │
│  │    (Nhận message từ Instance 1)                                             │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## SƠ ĐỒ CHI TIẾT QUEUE (Async Processing)

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              QUEUE PROCESSING FLOW                                  │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ Queue.Push(message)                                                          │   │
│  │   1. q.mu.Lock()                                                            │   │
│  │   2. q.messages = append(q.messages, msg)  ← Memory Storage                 │   │
│  │   3. data, _ := json.Marshal(msg)                                          │   │
│  │   4. q.writer.Write(append(data, '\n'))   ← File Storage                    │   │
│  │   5. q.writer.Flush()                                                       │   │
│  │   6. q.mu.Unlock()                                                          │   │
│  │   7. if q.redis != nil → go func() { q.redis.Publish() } ← Async Redis     │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                               │
│                                     ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ Queue.processor() (Goroutine)                                               │   │
│  │   - ticker := time.NewTicker(100ms)                                        │   │
│  │   - for { case <-ticker.C: → processMessages() }                           │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                     │                                               │
│                                     ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │ Queue.processMessages()                                                     │   │
│  │   for {                                                                     │   │
│  │     msg := q.Pop()   ← Lấy message đầu                                     │   │
│  │     if msg == nil → break                                                  │   │
│  │     handler := q.handlers[msg.Type]  ← Tìm handler                         │   │
│  │     if handler == nil → log "No handler"                                   │   │
│  │     msg.Status = "processing"                                              │   │
│  │     handler(*msg)   ← Xử lý business logic                                 │   │
│  │     msg.Status = "done"                                                    │   │
│  │   }                                                                         │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## SƠ ĐỒ LUỒNG DỮ LIỆU COMPLETE

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         COMPLETE DATA FLOW                                          │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  Client A                    Client B                Client C                       │
│  (Instance 1)               (Instance 1)            (Instance 2)                    │
│      │                           │                       │                          │
│      │  1. Send message          │                       │                          │
│      ├──────────────────────────▶│                       │                          │
│      │                           │                       │                          │
│      ▼                           │                       │                          │
│  ┌─────────────────────┐        │                       │                          │
│  │ ReadPump            │        │                       │                          │
│  └─────────────────────┘        │                       │                          │
│      │                           │                       │                          │
│      ▼                           │                       │                          │
│  ┌─────────────────────┐        │                       │                          │
│  │ HandleLocalMessage  │        │                       │                          │
│  └─────────────────────┘        │                       │                          │
│      │                           │                       │                          │
│      ├──2a. Broadcast Local─────▶│   ✅ Receive         │                          │
│      │                           │                       │                          │
│      ├──2b. Queue.Push()         │                       │                          │
│      │   └── Async Process       │                       │                          │
│      │                           │                       │                          │
│      └──2c. Broker.Publish()─────┼───────────────────────┼──▶ Redis                │
│                                  │                       │                          │
│                                  │                       │ 3. Redis Pub/Sub         │
│                                  │                       │    "ws-broadcast"        │
│                                  │                       │                          │
│                                  │                       │ 4. Broker.Subscribe()    │
│                                  │                       │    (Goroutine)           │
│                                  │                       │                          │
│                                  │                       ▼                          │
│                                  │               ┌─────────────────────┐          │
│                                  │               │ handleBrokerEvent   │          │
│                                  │               └─────────────────────┘          │
│                                  │                       │                          │
│                                  │                       ├──5a. Broadcast Local    │
│                                  │                       │    (Client C)           │
│                                  │                       │                          │
│                                  │                       └──5b. OnBrokerMessage    │
│                                  │                            (Callback)            │
│                                  │                       │                          │
│                                  │                       ▼                          │
│                                  │                   Client C                       │
│                                  │                 ✅ Receive                       │
│                                  │                                                  │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## BẢNG TÓM TẮT CÁC THÀNH PHẦN

| STT | Component | File | Chức năng |
|-----|-----------|------|-----------|
| 1 | **ReadPump** | internal/core/client_read.go | Đọc message từ WebSocket, parse action |
| 2 | **HandleLocalMessage** | internal/core/hub_broadcast.go | Xử lý message local, broadcast, queue, broker |
| 3 | **OnLocalMessage** | Callback | Business logic: validate, save DB, transform |
| 4 | **broadcastToLocalClients** | internal/core/hub_broadcast.go | Gửi message đến clients cùng instance |
| 5 | **Queue** | internal/queue/queue.go | Async processing với memory + file storage |
| 6 | **Broker.Publish** | internal/redisbroker/redis_broker.go | Publish message lên Redis |
| 7 | **Broker.Subscribe** | internal/redisbroker/redis_broker.go | Subscribe Redis channel |
| 8 | **handleBrokerEvent** | internal/core/hub_broadcast.go | Xử lý message từ Redis |
| 9 | **Client.SendBytes** | internal/core/client_send.go | Non-blocking send đến client |
| 10 | **WritePump** | internal/core/client_write.go | Ghi message vào WebSocket |

## LƯU Ý QUAN TRỌNG

1. **Message không bị duplicate** vì `handleBrokerEvent` **KHÔNG** publish lại broker
2. **Queue hoạt động async** với processor chạy mỗi 100ms
3. **Non-blocking send** giúp không block khi client buffer đầy
4. **Room support** trong `handleBrokerEvent` với `BroadcastToRoomLocal`
5. **Redis fallback** khi queue không được cấu hình