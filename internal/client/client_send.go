package client

import (
	"fmt"

	"github.com/DVV-15324/witches-realtime/internal/model"
)

func (c *Client) SendBytes(data []byte) error {
	select {
	case c.Send <- data:
	default:
		c.logger.Warn(fmt.Sprintf("send channel full, message dropped: id=%s, len=%d", c.ID, len(data)))
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
