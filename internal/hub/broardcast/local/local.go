package local

import (
	"realtime/internal/types"
)

func BroadcastToLocalClientsExclude(h types.IHub, message []byte, excludeID string) {
	h.GetMutex().RLock()
	clients := make([]types.IClient, 0, len(h.GetClients()))
	for id, c := range h.GetClients() {
		if id != excludeID {
			clients = append(clients, c)
		}
	}
	h.GetMutex().RUnlock()

	for _, client := range clients {
		if err := client.SendBytes(message); err != nil {
			h.GetLogger().Error("broadcastToLocalClientsExclude send error",
				"clientID", client.GetID(),
				"err", err)
		}
	}
}
