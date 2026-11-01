package client

import (
	"fmt"

	"github.com/DVV-15324/witches-realtime/internal/hub/action"
	"github.com/DVV-15324/witches-realtime/internal/hub/broardcast/local"
	"github.com/DVV-15324/witches-realtime/internal/hub/helper"
	"github.com/DVV-15324/witches-realtime/internal/model"
)

func (c *Client) HandleAction(req *model.Action, raw []byte) {
	switch req.Action {
	case "join":
		if req.RoomID == "" {
			c.logger.Warn(fmt.Sprintf("join action missing room_id, id=%s", c.ID))
			return
		}
		action.JoinRoom(c.Hub, c.ID, req.RoomID)
		c.logger.Info(fmt.Sprintf("joined room, id=%s, roomID=%s", c.ID, req.RoomID))

	case "leave":
		if req.RoomID == "" {
			c.logger.Warn(fmt.Sprintf("leave action missing room_id, id=%s", c.ID))
			return
		}
		action.LeaveRoom(c.Hub, c.ID, req.RoomID)
		c.logger.Info(fmt.Sprintf("left room, id=%s, roomID=%s", c.ID, req.RoomID))

	case "chat":
		c.HandleChat(req, raw)

	default:
		c.logger.Warn(fmt.Sprintf("unknown action, id=%s, action=%s", c.ID, req.Action))
	}
}

func (c *Client) HandleChat(req *model.Action, rawMessage []byte) {
	if req.TargetID != "" {
		privateRoom := getPrivateRoomID(c.GetUserID(), req.TargetID)
		action.JoinRoom(c.Hub, c.ID, privateRoom)
		if receiver := helper.GetClientByUserID(c.Hub, req.TargetID); receiver != nil {
			action.JoinRoom(c.Hub, receiver.GetID(), privateRoom)
		}
		local.BroadcastToRoomLocal(c.Hub, privateRoom, rawMessage)
		return
	}
	if req.RoomID == "" {
		c.logger.Warn(fmt.Sprintf("chat action missing room_id, id=%s", c.ID))
		return
	}
	local.BroadcastToRoomLocal(c.Hub, req.RoomID, rawMessage)
}

func getPrivateRoomID(userA, userB string) string {
	if userA < userB {
		return fmt.Sprintf("private:%s_%s", userA, userB)
	}
	return fmt.Sprintf("private:%s_%s", userB, userA)
}
