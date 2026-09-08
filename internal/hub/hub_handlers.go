package hub

import (
	"encoding/json"
	"realtime/internal/hub/broardcast/broker"
	"realtime/internal/hub/broardcast/local"
	"realtime/internal/queue"
	"realtime/internal/types"
	"time"
)

type LocalMessage struct {
	Client  types.IClient
	Message []byte
}

// HandleLocalMessage đưa message từ client vào event queue.
// Việc xử lý thực tế được thực hiện trong handleLocalMessage().
func (h *Hub) HandleLocalMessage(client types.IClient, message []byte) {
	if len(message) == 0 {
		return
	}

	h.localMessage <- LocalMessage{
		Client:  client,
		Message: message,
	}
}

// handleLocalMessage xử lý message từ client.
// Hàm này được gọi từ Hub event loop.
func (h *Hub) handleLocalMessage(msg LocalMessage) {
	client := msg.Client
	message := msg.Message

	// Enrich message
	var enriched map[string]interface{}
	if err := json.Unmarshal(message, &enriched); err == nil {
		enriched["__server_id"] = h.serverID
		enriched["__timestamp"] = time.Now().Unix()

		if data, err := json.Marshal(enriched); err == nil {
			message = data
		}
	}

	// 1. GHI FILE LOG
	if h.fileWriter != nil {
		if err := h.fileWriter.Write(message); err != nil {
			h.logger.Error(
				"failed to write message to file",
				"err", err,
			)
		}
	}

	// 2. CALLBACK
	if h.OnLocalMessage != nil {
		h.OnLocalMessage(client, message)
	}

	// 3. BROADCAST LOCAL - bỏ qua sender
	local.BroadcastToLocalClientsExclude(
		h,
		message,
		client.GetID(),
	)

	// 4. QUEUE / BROKER
	if msgQueue != nil {
		queueMsg := queue.NewMessage(
			"local",
			"",
			message,
			client.GetID(),
		)

		if err := msgQueue.Push(queueMsg); err != nil {
			h.logger.Error(
				"queue push error",
				"err", err,
			)
		}

		return
	}

	if h.broker != nil {
		if err := h.broker.Publish(message); err != nil {
			h.logger.Error(
				"broker publish error",
				"err", err,
			)
		}
	}
}

// handleBrokerEvent xử lý message nhận từ broker.
func (h *Hub) handleBrokerEvent(message []byte) {
	if len(message) == 0 {
		return
	}

	// Kiểm tra message có phải từ chính server này không.
	var meta map[string]interface{}

	if err := json.Unmarshal(message, &meta); err == nil {
		if serverID, ok := meta["__server_id"].(string); ok &&
			serverID == h.serverID {

			h.logger.Debug(
				"skipping message from same server",
				"serverID", serverID,
			)

			return
		}
	}

	// 1. GHI FILE LOG
	if h.fileWriter != nil {
		if err := h.fileWriter.Write(message); err != nil {
			h.logger.Error(
				"failed to write broker message to file",
				"err", err,
			)
		}
	}

	// 2. Kiểm tra message có room không.
	var wrapped struct {
		Room    string          `json:"room"`
		Payload json.RawMessage `json:"payload"`
	}

	if err := json.Unmarshal(message, &wrapped); err == nil &&
		wrapped.Room != "" {

		if h.OnBrokerMessage != nil {
			h.OnBrokerMessage(wrapped.Payload)
		}

		local.BroadcastToRoomLocal(
			h,
			wrapped.Room,
			wrapped.Payload,
		)

		if msgQueue != nil {
			queueMsg := queue.NewMessage(
				"broker",
				wrapped.Room,
				wrapped.Payload,
				"",
			)

			if err := msgQueue.Push(queueMsg); err != nil {
				h.logger.Error(
					"queue push error (broker room)",
					"err", err,
				)
			}
		}

		return
	}

	// 3. Message thông thường, không có room.
	if h.OnBrokerMessage != nil {
		h.OnBrokerMessage(message)
	}

	broker.BroadcastToBrokerClients(h, message)

	if msgQueue != nil {
		queueMsg := queue.NewMessage(
			"broker",
			"",
			message,
			"",
		)

		if err := msgQueue.Push(queueMsg); err != nil {
			h.logger.Error(
				"queue push error (broker)",
				"err", err,
			)
		}
	}
}
