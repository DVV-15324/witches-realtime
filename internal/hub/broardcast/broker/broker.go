package broker

import (
	"fmt"

	"github.com/DVV-15324/witches-realtime/internal/model"
	"github.com/DVV-15324/witches-realtime/internal/queue"
	"github.com/DVV-15324/witches-realtime/internal/types"
)

func BroadcastToAllAndBroker(h types.IHub, msg model.Message, msgQueue *queue.Queue) {
	data, err := msg.ToJSON()
	if err != nil {
		h.GetLogger().Error(fmt.Sprintf("marshal error: %v", err))
		return
	}

	BroadcastToBrokerClients(h, data)

	if msgQueue != nil {
		queueMsg := queue.NewMessage("broker", "", data, "")
		if err := msgQueue.Push(queueMsg); err != nil {
			h.GetLogger().Error(fmt.Sprintf("queue push error: %v", err))
		}
	} else {
		if h.GetBroker() != nil {
			if err := h.GetBroker().Publish(data); err != nil {
				h.GetLogger().Error(fmt.Sprintf("broker publish error: %v", err))
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
			h.GetLogger().Error(fmt.Sprintf("broadcastBrokerClients send error, clientID=%s, err=%v", client.GetID(), err))
		}
	}
}
