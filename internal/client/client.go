package client

import (
	"realtime/internal/model"
	"realtime/internal/types"

	"github.com/gorilla/websocket"
)

const sendBufferSize = 10000

type Client struct {
	ID       string
	UserID   string
	Conn     *websocket.Conn
	Action   *model.Action
	Hub      types.IHub
	Send     chan []byte
	Metadata map[string]interface{}
	ready    chan struct{}
	logger   types.ILogger
}

func NewClient(id, userID string, conn *websocket.Conn, hub types.IHub, metadata map[string]interface{}) *Client {
	return &Client{
		ID:       id,
		UserID:   userID,
		Conn:     conn,
		Hub:      hub,
		Send:     make(chan []byte, sendBufferSize),
		Metadata: metadata,
		logger:   hub.GetLogger(),
		ready:    make(chan struct{}),
	}
}
func (c *Client) GetID() string                       { return c.ID }
func (c *Client) GetAction() string                   { return c.Action.Action }
func (c *Client) GetRoomID() string                   { return c.Action.RoomID }
func (c *Client) GetTargetID() string                 { return c.Action.TargetID }
func (c *Client) GetUserID() string                   { return c.UserID }
func (c *Client) GetMetadata() map[string]interface{} { return c.Metadata }
func (c *Client) Close() error                        { return c.Conn.Close() }
func (c *Client) WaitReady()                          { <-c.ready }
