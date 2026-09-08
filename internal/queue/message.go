package queue

// import (
// 	"encoding/json"
// 	"time"
// )

// // Message định nghĩa message trong queue
// type Message struct {
// 	ID        string          `json:"id"`
// 	Type      string          `json:"type"` // "local", "broker"
// 	Room      string          `json:"room"`
// 	Payload   json.RawMessage `json:"payload"`
// 	ClientID  string          `json:"client_id"`
// 	Timestamp time.Time       `json:"timestamp"`
// 	Retry     int             `json:"retry"`
// 	Status    string          `json:"status"` // "pending", "processing", "done", "failed"
// }

// // NewMessage tạo message mới
// func NewMessage(msgType, room string, payload []byte, clientID string) Message {
// 	return Message{
// 		ID:        generateID(),
// 		Type:      msgType,
// 		Room:      room,
// 		Payload:   payload,
// 		ClientID:  clientID,
// 		Timestamp: time.Now(),
// 		Retry:     0,
// 		Status:    "pending",
// 	}
// }

// func generateID() string {
// 	return time.Now().Format("20060102150405") + "-" + randomString(6)
// }

// func randomString(n int) string {
// 	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
// 	b := make([]byte, n)
// 	for i := range b {
// 		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
// 	}
// 	return string(b)
// }
