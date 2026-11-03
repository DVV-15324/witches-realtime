package model

import "encoding/json"

type Message struct {
	Type     string          `json:"type"`
	RoomID   string          `json:"room_id,omitempty"`
	TargetID string          `json:"target_id,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Except   string          `json:"except,omitempty"`
}

func (m *Message) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

func FromJSON(data []byte) (*Message, error) {
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
