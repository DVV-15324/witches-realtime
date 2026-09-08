package client

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"realtime/internal/model"
	"time"
)

func (c *Client) ReadPump() {
	defer func() {
		c.Hub.GetUnRegister() <- c
		c.Conn.Close()
		c.logger.Info("ReadPump stopped", "id", c.ID)
	}()

	c.logger.Info("ReadPump started", "id", c.ID)
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.logger.Error("read error", "id", c.ID, "err", err)
			} else {
				c.logger.Info("read closed", "id", c.ID, "err", err)
			}
			break
		}

		if len(message) == 0 {
			continue
		}

		c.processMessage(message)
	}
}

func (c *Client) processMessage(message []byte) {
	var req model.Action
	if err := json.Unmarshal(message, &req); err == nil && req.Action != "" {
		c.HandleAction(&req, message)
		return
	}
	c.Hub.HandleLocalMessage(c, message)
}
