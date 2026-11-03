package hub

import (
	"fmt"

	"github.com/DVV-15324/witches-realtime/pkg/model"
	"github.com/DVV-15324/witches-realtime/pkg/types"
)

func (h *Hub) handleBroadcast(data []byte) {
	msg, err := model.FromJSON(data)
	if err != nil {
		h.logger.Error(fmt.Sprintf("parse broadcast: %v", err))
		return
	}
	if msg.TargetID != "" {
		h.broadcastToUser(msg.TargetID, msg.Payload)
		return
	}
	if msg.RoomID != "" {
		if msg.Except != "" {
			h.broadcastRoomExcept(msg.RoomID, msg.Except, msg.Payload)
		} else {
			h.broadcastRoom(msg.RoomID, msg.Payload)
		}
		return
	}
	if msg.Except != "" {
		h.broadcastAllExcept(msg.Except, msg.Payload)
	} else {
		h.broadcastAll(msg.Payload)
	}
}

func (h *Hub) broadcastToUser(userID string, data []byte) {
	h.mu.RLock()
	clients := h.byUser[userID]
	targets := make([]types.IClient, 0, len(clients))
	for _, c := range clients {
		targets = append(targets, c)
	}
	h.mu.RUnlock()
	for _, c := range targets {
		_ = c.Send(data)
	}
}

func (h *Hub) broadcastAll(data []byte) {
	h.mu.RLock()
	clients := make([]types.IClient, 0, len(h.clients))
	for _, c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		_ = c.Send(data)
	}
}

func (h *Hub) broadcastAllExcept(exceptUserID string, data []byte) {
	h.mu.RLock()
	clients := make([]types.IClient, 0, len(h.clients))
	for _, c := range h.clients {
		if c.UserID() != exceptUserID {
			clients = append(clients, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range clients {
		_ = c.Send(data)
	}
}

func (h *Hub) broadcastRoom(roomID string, data []byte) {
	h.mu.RLock()
	set := h.rooms[roomID]
	clients := make([]types.IClient, 0, len(set))
	for _, c := range set {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		_ = c.Send(data)
	}
}

func (h *Hub) broadcastRoomExcept(roomID, exceptUserID string, data []byte) {
	h.mu.RLock()
	set := h.rooms[roomID]
	clients := make([]types.IClient, 0, len(set))
	for _, c := range set {
		if c.UserID() != exceptUserID {
			clients = append(clients, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range clients {
		_ = c.Send(data)
	}
}
