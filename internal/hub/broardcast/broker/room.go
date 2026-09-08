package broker

import (
	"encoding/json"
	"realtime/internal/hub/broardcast/local"
	"realtime/internal/types"
)

// BroadcastToRoomAndBroker: gửi message đến room và publish lên broker
func BroadcastToRoomAndBroker(h types.IHub, room string, msg []byte) {
	// 1. Broadcast local
	local.BroadcastToRoomLocal(h, room, msg)
	// 2. Publish lên broker (có kèm room info)
	if h.GetBroker() != nil {
		wrappedMsg := struct {
			Room    string `json:"room"`
			Payload []byte `json:"payload"`
		}{
			Room:    room,
			Payload: msg,
		}
		data, err := json.Marshal(wrappedMsg)
		if err != nil {
			h.GetLogger().Error("marshal room message error", "err", err)
			return
		}
		if err := h.GetBroker().Publish(data); err != nil {
			h.GetLogger().Error("broker publish error", "err", err)
		}
	}
}
