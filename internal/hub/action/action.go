package action

import (
	"realtime/internal/types"
)

func JoinRoom(h types.IHub, clientID, room string) {
	h.GetMutex().Lock()
	defer h.GetMutex().Unlock()

	if _, ok := h.GetRooms()[room]; !ok {
		h.GetRooms()[room] = make(map[string]types.IClient)
	}
	if client, ok := h.GetClients()[clientID]; ok {
		h.GetRooms()[room][clientID] = client
		h.GetLogger().Info("client joined room", "clientID", clientID, "room", room)
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
		h.GetLogger().Info("client left room", "clientID", clientID, "room", room)
	}
}
