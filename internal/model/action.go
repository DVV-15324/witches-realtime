package model

import "encoding/json"

type Action struct {
	Action   string          `json:"action"`
	RoomID   string          `json:"room_id,omitempty"`
	TargetID string          `json:"target_id,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}
