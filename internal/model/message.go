package model

import "encoding/json"

type Message interface {
	GetType() string
	GetData() interface{}
	ToJSON() ([]byte, error)
}

type BaseMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

func (m BaseMessage) GetType() string         { return m.Type }
func (m BaseMessage) GetData() interface{}    { return m.Data }
func (m BaseMessage) ToJSON() ([]byte, error) { return json.Marshal(m) }

type ChatMessage struct {
	Username string `json:"username"`
	Message  string `json:"message"`
	Time     string `json:"time"`
}

func (c ChatMessage) GetType() string         { return "chat" }
func (c ChatMessage) GetData() interface{}    { return c }
func (c ChatMessage) ToJSON() ([]byte, error) { return json.Marshal(c) }
