package client

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/DVV-15324/witches-realtime/internal/model"
	"github.com/gorilla/websocket"
)

func (c *Client) ReadPump() {
	defer func() {
		c.Hub.GetUnRegister() <- c
		c.Conn.Close()
		c.logger.Info(fmt.Sprintf("ReadPump stopped, id=%s", c.ID))
	}()

	c.logger.Info(fmt.Sprintf("ReadPump started, id=%s", c.ID))
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.logger.Error(fmt.Sprintf("read error, id=%s, err=%v", c.ID, err))
			} else {
				c.logger.Info(fmt.Sprintf("read closed, id=%s, err=%v", c.ID, err))
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
