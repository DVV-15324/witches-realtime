package client

import (
	"realtime/internal/hub/action"
	"realtime/internal/hub/broardcast/local"
	"realtime/internal/hub/helper"
	"realtime/internal/model"
)

func (c *Client) HandleAction(req *model.Action, raw []byte) {
	switch req.Action {
	case "join":
		if req.RoomID == "" {
			c.logger.Warn(
				"join action missing room_id",
				"id", c.ID,
			)
			return
		}
		action.JoinRoom(c.Hub, c.ID, req.RoomID)
		c.logger.Info("joined room", "id", c.ID, "roomID", req.RoomID)
	case "leave":
		if req.RoomID == "" {
			c.logger.Warn("leave action missing room_id", "id", c.ID)
			return
		}
		action.LeaveRoom(c.Hub, c.ID, req.RoomID)
		c.logger.Info("left room", "id", c.ID, "roomID", req.RoomID)
	case "chat":
		c.HandleChat(req, raw)
	default:
		c.logger.Warn("unknown action", "id", c.ID, "action", req.Action)
	}
}
func (c *Client) HandleChat(req *model.Action, rawMessage []byte) {
	if req.TargetID != "" {
		privateRoom := getPrivateRoomID(
			c.GetUserID(),
			req.TargetID,
		)
		action.JoinRoom(c.Hub, c.ID, privateRoom)
		if receiver := helper.GetClientByUserID(c.Hub, req.TargetID); receiver != nil {
			action.JoinRoom(c.Hub, receiver.GetID(), privateRoom)
		}
		local.BroadcastToRoomLocal(c.Hub, privateRoom, rawMessage)
		return
	}
	if req.RoomID == "" {
		c.logger.Warn("chat action missing room_id", "id", c.ID)
		return
	}
	local.BroadcastToRoomLocal(c.Hub, req.RoomID, rawMessage)
}

func getPrivateRoomID(userA, userB string) string {
	if userA < userB {
		return "private:" + userA + "_" + userB
	}
	return "private:" + userB + "_" + userA
}
