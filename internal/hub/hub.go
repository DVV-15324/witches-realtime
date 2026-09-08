package hub

import (
	"context"
	"realtime/internal/persist"
	"realtime/internal/types"
	"sync"
)

type Hub struct {
	// Core
	clients   map[string]types.IClient
	rooms     map[string]map[string]types.IClient
	mu        sync.RWMutex
	logger    types.ILogger
	broker    types.IBroker
	ctx       context.Context
	broadcast chan []byte
	serverID  string

	// Persist
	fileWriter *persist.FileWriter

	// Channels
	register     chan types.IClient
	unregister   chan types.IClient
	localMessage chan LocalMessage

	// Callbacks
	OnLocalMessage  func(types.IClient, []byte)
	OnBrokerMessage func([]byte)
	OnConnect       func(types.IClient)
	OnDisconnect    func(types.IClient)
}

func NewHub(
	logger types.ILogger,
	broker types.IBroker,
	ctx context.Context,
	fileWriter *persist.FileWriter,
	serverID string,
) *Hub {
	return &Hub{
		clients:      make(map[string]types.IClient),
		rooms:        make(map[string]map[string]types.IClient),
		broadcast:    make(chan []byte, 4096),
		register:     make(chan types.IClient, 256),
		localMessage: make(chan LocalMessage, 4096),
		unregister:   make(chan types.IClient, 256),
		logger:       logger,
		broker:       broker,
		ctx:          ctx,
		fileWriter:   fileWriter,
		serverID:     serverID,
	}
}

func (h *Hub) GetBrokerBroadcast() chan []byte                { return h.broadcast }
func (h *Hub) GetOnLocalMessage() func(types.IClient, []byte) { return h.OnLocalMessage }
func (h *Hub) GetOnBrokerMessage() func([]byte)               { return h.OnBrokerMessage }
func (h *Hub) GetOnConnect() func(types.IClient)              { return h.OnConnect }
func (h *Hub) GetOnDisconnect() func(types.IClient)           { return h.OnDisconnect }
func (h *Hub) GetLogger() types.ILogger                       { return h.logger }
func (h *Hub) GetBroker() types.IBroker                       { return h.broker }
func (h *Hub) GetContext() context.Context                    { return h.ctx }
func (h *Hub) GetMutex() *sync.RWMutex                        { return &h.mu }
func (h *Hub) GetRooms() map[string]map[string]types.IClient  { return h.rooms }
func (h *Hub) GetClients() map[string]types.IClient           { return h.clients }
func (h *Hub) GetRegister() chan types.IClient                { return h.register }
func (h *Hub) GetUnRegister() chan types.IClient              { return h.unregister }
