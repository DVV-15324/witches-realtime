package local

import (
	"realtime/internal/types"
)

func BroadcastToRoomLocal(h types.IHub, room string, msg []byte) {
	h.GetMutex().RLock()
	members, ok := h.GetRooms()[room]
	h.GetMutex().RUnlock()
	if !ok {
		return
	}

	for _, client := range members {
		if err := client.SendBytes(msg); err != nil {
			h.GetLogger().Error("BroadcastToRoomLocal send error",
				"room", room,
				"clientID", client.GetID(),
				"err", err)
		}
	}
}
