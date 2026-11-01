package local

import (
	"fmt"

	"github.com/DVV-15324/witches-realtime/internal/types"
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
			h.GetLogger().Error(fmt.Sprintf("broadcastToLocalClientsExclude send error, clientID=%s, err=%v", client.GetID(), err))
		}
	}
}
