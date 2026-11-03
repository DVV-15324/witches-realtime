package hub

import (
	"fmt"
	"sync"

	"github.com/DVV-15324/witches-realtime/pkg/types"
)

// Thêm struct request
type registerReq struct {
	client types.IClient
	rooms  []string
}

type Hub struct {
	mu      sync.RWMutex
	clients map[string]types.IClient
	byUser  map[string]map[string]types.IClient
	rooms   map[string]map[string]types.IClient
	logger  types.ILogger

	register   chan registerReq // <-- Đổi thành registerReq
	unregister chan types.IClient
	broadcast  chan []byte
	stop       chan struct{}
}

func New(logger types.ILogger) *Hub {
	return &Hub{
		clients:    make(map[string]types.IClient),
		byUser:     make(map[string]map[string]types.IClient),
		rooms:      make(map[string]map[string]types.IClient),
		logger:     logger,
		register:   make(chan registerReq, 256),
		unregister: make(chan types.IClient, 256),
		broadcast:  make(chan []byte, 4096),
		stop:       make(chan struct{}),
	}
}

func (h *Hub) Register(c types.IClient, rooms []string) {
	h.register <- registerReq{client: c, rooms: rooms}
}

func (h *Hub) Unregister(c types.IClient) { h.unregister <- c }

func (h *Hub) PushBroadcast(data []byte) {
	select {
	case h.broadcast <- data:
	default:
		h.logger.Warn("hub broadcast channel full")
	}
}

func (h *Hub) Stop() { close(h.stop) }

func (h *Hub) Run() {
	h.logger.Info("Hub started")
	defer h.logger.Info("Hub stopped")

	for {
		select {
		case <-h.stop:
			h.cleanup()
			return

		case req := <-h.register:
			h.mu.Lock()
			c := req.client
			h.clients[c.ID()] = c
			if h.byUser[c.UserID()] == nil {
				h.byUser[c.UserID()] = make(map[string]types.IClient)
			}
			h.byUser[c.UserID()][c.ID()] = c
			for _, roomID := range req.rooms {
				if h.rooms[roomID] == nil {
					h.rooms[roomID] = make(map[string]types.IClient)
				}
				h.rooms[roomID][c.ID()] = c
			}
			h.mu.Unlock()

			h.logger.Info(fmt.Sprintf("registered: id=%s user=%s rooms=%v", c.ID(), c.UserID(), req.rooms))

		case c := <-h.unregister:
			h.mu.Lock()
			delete(h.clients, c.ID())
			if set := h.byUser[c.UserID()]; set != nil {
				delete(set, c.ID())
				if len(set) == 0 {
					delete(h.byUser, c.UserID())
				}
			}
			for roomID, set := range h.rooms {
				if _, ok := set[c.ID()]; ok {
					delete(set, c.ID())
					if len(set) == 0 {
						delete(h.rooms, roomID)
					}
				}
			}
			h.mu.Unlock()
			h.logger.Info(fmt.Sprintf("unregistered: id=%s user=%s", c.ID(), c.UserID()))

		case data := <-h.broadcast:
			h.handleBroadcast(data)
		}
	}
}

func (h *Hub) cleanup() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.clients {
		_ = c.Close()
	}
	h.clients = make(map[string]types.IClient)
	h.byUser = make(map[string]map[string]types.IClient)
	h.rooms = make(map[string]map[string]types.IClient)
}

func (h *Hub) JoinRoom(userID, roomID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[roomID] == nil {
		h.rooms[roomID] = make(map[string]types.IClient)
	}
	for _, c := range h.byUser[userID] {
		h.rooms[roomID][c.ID()] = c
	}
}

func (h *Hub) LeaveRoom(userID, roomID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if set := h.rooms[roomID]; set != nil {
		for _, c := range h.byUser[userID] {
			delete(set, c.ID())
		}
		if len(set) == 0 {
			delete(h.rooms, roomID)
		}
	}
}
