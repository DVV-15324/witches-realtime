package action

import (
	"fmt"

	"github.com/DVV-15324/witches-realtime/internal/types"
)

func JoinRoom(h types.IHub, clientID, room string) {
	h.GetMutex().Lock()
	defer h.GetMutex().Unlock()

	if _, ok := h.GetRooms()[room]; !ok {
		h.GetRooms()[room] = make(map[string]types.IClient)
	}
	if client, ok := h.GetClients()[clientID]; ok {
		h.GetRooms()[room][clientID] = client
		h.GetLogger().Info(fmt.Sprintf("client joined room, clientID=%s, room=%s", clientID, room))
	}
}

func LeaveRoom(h types.IHub, clientID, room string) {
	h.GetMutex().Lock()
	defer h.GetMutex().Unlock()

	if members, ok := h.GetRooms()[room]; ok {
		delete(members, clientID)
		if len(members) == 0 {
			delete(h.GetRooms(), room)
		}
		h.GetLogger().Info(fmt.Sprintf("client left room, clientID=%s, room=%s", clientID, room))
	}
}
