package client

import (
	"realtime/internal/model"
)

func (c *Client) SendBytes(data []byte) error {
	select {
	case c.Send <- data:
	default:
		c.logger.Warn("send channel full, message dropped", "id", c.ID, "len", len(data))
	}
	return nil
}

func (c *Client) SendMessage(msg model.Message) error {
	data, err := msg.ToJSON()
	if err != nil {
		return err
	}
	return c.SendBytes(data)
}
