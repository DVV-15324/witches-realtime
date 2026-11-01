package client

import (
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

func (c *Client) WritePump() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error(fmt.Sprintf("WritePump panic, id=%s, recover=%v", c.ID, r))
		}
		c.Conn.Close()
		c.logger.Info(fmt.Sprintf("WritePump stopped, id=%s", c.ID))
	}()

	c.logger.Info(fmt.Sprintf("WritePump started, id=%s", c.ID))
	close(c.ready)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case message, ok := <-c.Send:
			if !ok {
				c.logger.Info(fmt.Sprintf("Send channel closed, id=%s", c.ID))
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				c.logger.Error(fmt.Sprintf("write error, id=%s, err=%v", c.ID, err))
				return
			}
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.logger.Error(fmt.Sprintf("ping error, id=%s, err=%v", c.ID, err))
				return
			}
		}
	}
}
