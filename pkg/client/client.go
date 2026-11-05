package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/DVV-15324/witches-realtime/pkg/types"
	"github.com/gorilla/websocket"
)

// Thêm kiểu dữ liệu cho callback
type MessageHandler func(userID string, msg []byte)

type Client struct {
	id        string
	userID    string
	conn      *websocket.Conn
	send      chan []byte
	logger    types.ILogger
	closed    chan struct{}
	closeOnce sync.Once
	onMessage MessageHandler
}

func New(id, userID string, conn *websocket.Conn, onMessage MessageHandler, logger types.ILogger) *Client {
	return &Client{
		id:     id,
		userID: userID,
		conn:   conn,
		send:   make(chan []byte, 256),
		logger: logger,
		closed: make(chan struct{}),
	}
}

func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		err = c.conn.Close()
	})
	return err
}

func (c *Client) ID() string     { return c.id }
func (c *Client) UserID() string { return c.userID }

func (c *Client) Send(data []byte) error {
	select {
	case c.send <- data:
		return nil
	case <-c.closed:
		return fmt.Errorf("client closed")
	default:
		c.logger.Warn(fmt.Sprintf("client %s buffer full, dropping", c.id))
		return fmt.Errorf("buffer full")
	}
}

func (c *Client) WritePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.Close()
		c.logger.Info(fmt.Sprintf("WritePump stopped, id=%s", c.id))
	}()
	c.logger.Info(fmt.Sprintf("WritePump started, id=%s", c.id))
	for {
		select {
		case <-c.closed:
			return
		case msg := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				c.logger.Error(fmt.Sprintf("write error, id=%s, err=%v", c.id, err))
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.logger.Error(fmt.Sprintf("ping error, id=%s, err=%v", c.id, err))
				return
			}
		}
	}
}

func (c *Client) ReadPump(onClose func(types.IClient)) {
	defer func() {
		onClose(c)
		c.Close()
		c.logger.Info(fmt.Sprintf("ReadPump stopped, id=%s", c.id))
	}()

	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			c.logger.Info(fmt.Sprintf("read closed, id=%s, err=%v", c.id, err))
			return
		}
		if len(msg) > 0 && c.onMessage != nil {
			c.onMessage(c.userID, msg)
		}
	}
}
