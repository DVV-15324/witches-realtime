package broker

import (
	"realtime/internal/model"
	"realtime/internal/queue"
	"realtime/internal/types"
)

func BroadcastToAllAndBroker(h types.IHub, msg model.Message, msgQueue *queue.Queue) {
	data, err := msg.ToJSON()
	if err != nil {
		h.GetLogger().Error("marshal error", "err", err)
		return
	}

	BroadcastToBrokerClients(h, data)

	if msgQueue != nil {
		queueMsg := queue.NewMessage("broker", "", data, "")
		if err := msgQueue.Push(queueMsg); err != nil {
			h.GetLogger().Error("queue push error", "err", err)
		}
	} else {
		if h.GetBroker() != nil {
			if err := h.GetBroker().Publish(data); err != nil {
				h.GetLogger().Error("broker publish error", "err", err)
			}
		}
	}
}

func BroadcastToBrokerClients(h types.IHub, message []byte) {
	h.GetMutex().RLock()
	clients := make([]types.IClient, 0, len(h.GetClients()))
	for _, c := range h.GetClients() {
		clients = append(clients, c)
	}
	h.GetMutex().RUnlock()

	for _, client := range clients {
		if err := client.SendBytes(message); err != nil {
			h.GetLogger().Error("broadcastBrokerClients send error",
				"clientID", client.GetID(),
				"err", err)
		}
	}
}
