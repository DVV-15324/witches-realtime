package hub

import (
	"fmt"
	"sync"
	"time"

	"github.com/DVV-15324/witches-realtime/pkg/types"
)

type registerReq struct {
	client types.IClient
	rooms  []string
}

type Options struct {
	EnableQueue      bool
	BroadcastWorkers int
}

type Hub struct {
	mu sync.RWMutex

	clients map[string]types.IClient
	byUser  map[string]map[string]types.IClient
	rooms   map[string]map[string]types.IClient

	logger types.ILogger

	register   chan registerReq
	unregister chan types.IClient
	broadcast  chan []byte

	stop     chan struct{}
	stopOnce sync.Once

	// Broadcast worker pool
	enableQueue      bool
	broadcastWorkers int
	workerWG         sync.WaitGroup

	pushMu  sync.RWMutex
	stopped bool
}

func New(logger types.ILogger) *Hub {
	return NewWithOptions(logger, Options{})
}

func NewWithOptions(logger types.ILogger, opts Options) *Hub {
	workers := opts.BroadcastWorkers
	if workers <= 0 {
		workers = 4
	}

	return &Hub{
		clients: make(map[string]types.IClient),
		byUser:  make(map[string]map[string]types.IClient),
		rooms:   make(map[string]map[string]types.IClient),

		logger: logger,

		register:   make(chan registerReq, 256),
		unregister: make(chan types.IClient, 256),
		broadcast:  make(chan []byte, 4096),

		stop: make(chan struct{}),

		enableQueue:      opts.EnableQueue,
		broadcastWorkers: workers,
	}
}
func (h *Hub) registerClient(c types.IClient, rooms []string) {
	clientID := c.ID()
	userID := c.UserID()

	h.clients[clientID] = c

	if h.byUser[userID] == nil {
		h.byUser[userID] = make(map[string]types.IClient)
	}

	h.byUser[userID][clientID] = c

	for _, roomID := range rooms {
		if h.rooms[roomID] == nil {
			h.rooms[roomID] = make(map[string]types.IClient)
		}

		h.rooms[roomID][clientID] = c
	}

	h.logger.Info(fmt.Sprintf(
		"client registered: user=%s client=%s rooms=%v",
		userID,
		clientID,
		rooms,
	))
}

func (h *Hub) unregisterClient(c types.IClient) {
	clientID := c.ID()
	userID := c.UserID()

	delete(h.clients, clientID)

	if userClients := h.byUser[userID]; userClients != nil {
		delete(userClients, clientID)

		if len(userClients) == 0 {
			delete(h.byUser, userID)
		}
	}

	for roomID, roomClients := range h.rooms {
		delete(roomClients, clientID)

		if len(roomClients) == 0 {
			delete(h.rooms, roomID)
		}
	}

	h.logger.Info(fmt.Sprintf(
		"client unregistered: user=%s client=%s",
		userID,
		clientID,
	))
}
func (h *Hub) Register(c types.IClient, rooms []string) {
	h.register <- registerReq{client: c, rooms: rooms}
}

func (h *Hub) Unregister(c types.IClient) { h.unregister <- c }

func (h *Hub) PushBroadcast(data []byte) {
	payload := append([]byte(nil), data...)

	h.pushMu.RLock()
	defer h.pushMu.RUnlock()

	if h.stopped {
		h.logger.Warn("broadcast rejected: hub stopped")
		return
	}

	select {
	case h.broadcast <- payload:
		h.logger.Info(fmt.Sprintf(
			"[QUEUE] ENQUEUED | queue=%d/%d | payload=%s",
			len(h.broadcast),
			cap(h.broadcast),
			string(payload),
		))

	default:
		h.logger.Warn(fmt.Sprintf(
			"[QUEUE] FULL | queue=%d/%d | message dropped",
			len(h.broadcast),
			cap(h.broadcast),
		))
	}
}

func (h *Hub) Stop() {
	h.stopOnce.Do(func() {
		h.pushMu.Lock()
		h.stopped = true
		close(h.stop)
		h.pushMu.Unlock()
	})
}

func (h *Hub) Run() {
	h.logger.Info("hub started")

	broadcastCh := h.broadcast

	if h.enableQueue {
		broadcastCh = nil
		h.startBroadcastWorkers()
	}

	defer func() {
		if h.enableQueue {
			h.workerWG.Wait()
		}

		h.cleanup()
		h.logger.Info("hub stopped")
	}()

	for {
		select {
		case <-h.stop:
			return

		case req := <-h.register:
			h.mu.Lock()
			h.registerClient(req.client, req.rooms)
			h.mu.Unlock()

		case c := <-h.unregister:
			h.mu.Lock()
			h.unregisterClient(c)
			h.mu.Unlock()

		case data := <-broadcastCh:
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
func (h *Hub) startBroadcastWorkers() {
	for i := 0; i < h.broadcastWorkers; i++ {
		h.workerWG.Add(1)

		go func(workerID int) {
			defer h.workerWG.Done()

			h.logger.Info(fmt.Sprintf(
				"[WORKER %d] STARTED",
				workerID,
			))

			process := func(data []byte) {
				h.logger.Info(fmt.Sprintf(
					"[WORKER %d] RECEIVED | payload=%s",
					workerID,
					string(data),
				))

				h.handleBroadcast(data)
				time.Sleep(500 * time.Millisecond)
				h.logger.Info(fmt.Sprintf(
					"[WORKER %d] COMPLETED",
					workerID,
				))
			}

			for {
				select {
				case data := <-h.broadcast:
					process(data)

				case <-h.stop:
					for {
						select {
						case data := <-h.broadcast:
							process(data)

						default:
							h.logger.Info(fmt.Sprintf(
								"[WORKER %d] STOPPED",
								workerID,
							))
							return
						}
					}
				}
			}
		}(i)
	}
}
