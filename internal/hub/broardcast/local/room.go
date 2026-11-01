package local

import (
	"fmt"

	"github.com/DVV-15324/witches-realtime/internal/types"
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
			h.GetLogger().Error(fmt.Sprintf("BroadcastToRoomLocal send error, room=%s, clientID=%s, err=%v", room, client.GetID(), err))
		}
	}
}
