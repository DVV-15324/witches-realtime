package broker

import (
	"encoding/json"
	"fmt"

	"github.com/DVV-15324/witches-realtime/internal/hub/broardcast/local"
	"github.com/DVV-15324/witches-realtime/internal/types"
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
			h.GetLogger().Error(fmt.Sprintf("marshal room message error: %v", err))
			return
		}
		if err := h.GetBroker().Publish(data); err != nil {
			h.GetLogger().Error(fmt.Sprintf("broker publish error: %v", err))
		}
	}
}
