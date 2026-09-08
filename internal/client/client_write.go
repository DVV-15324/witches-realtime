package client

import (
	"github.com/gorilla/websocket"
	"time"
)

func (c *Client) WritePump() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("WritePump panic", "id", c.ID, "recover", r)
		}
		c.Conn.Close()
		c.logger.Info("WritePump stopped", "id", c.ID)
	}()

	c.logger.Info("WritePump started", "id", c.ID)
	close(c.ready)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case message, ok := <-c.Send:
			if !ok {
				c.logger.Info("Send channel closed", "id", c.ID)
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				c.logger.Error("write error", "id", c.ID, "err", err)
				return
			}
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.logger.Error("ping error", "id", c.ID, "err", err)
				return
			}
		}
	}
}
