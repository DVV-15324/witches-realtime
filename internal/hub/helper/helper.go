package helper

import (
	"realtime/internal/hub/broardcast/broker"
	"realtime/internal/model"
	"realtime/internal/types"
)

func SendToClient(h types.IHub, clientID string, msg model.Message) error {
	h.GetMutex().RLock()
	client, ok := h.GetClients()[clientID]
	h.GetMutex().RUnlock()
	if !ok {
		return nil
	}
	data, err := msg.ToJSON()
	if err != nil {
		return err
	}
	return client.SendBytes(data)
}

func GetClientByUserID(h types.IHub, userID string) types.IClient {
	h.GetMutex().RLock()
	defer h.GetMutex().RUnlock()

	for _, client := range h.GetClients() {
		if client.GetUserID() == userID {
			return client
		}
	}

	return nil
}

func GetOnlineClients(h types.IHub) []string {
	h.GetMutex().RLock()
	defer h.GetMutex().RUnlock()

	ids := make([]string, 0, len(h.GetClients()))
	for id := range h.GetClients() {
		ids = append(ids, id)
	}
	return ids
}

func GetClientCount(h types.IHub) int {
	h.GetMutex().RLock()
	defer h.GetMutex().RUnlock()

	return len(h.GetClients())
}

func CloseAll(h types.IHub) {
	h.GetMutex().Lock()
	defer h.GetMutex().Unlock()

	for _, client := range h.GetClients() {
		client.Close()
	}

	if h.GetBroker() != nil {
		h.GetBroker().Close()
	}

	h.GetLogger().Info("all GetClients() closed")
}

func BroadcastToAll(h types.IHub, msg model.Message) {
	data, err := msg.ToJSON()
	if err != nil {
		h.GetLogger().Error("marshal error", "err", err)
		return
	}
	broker.BroadcastToBrokerClients(h, data)
}
