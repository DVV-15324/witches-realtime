package hub

import (
	"net/http"
	"realtime/internal/client"
	"realtime/internal/config"
	"realtime/internal/hub/broardcast/broker"
	"realtime/internal/hub/helper"
	"realtime/internal/model"
	"realtime/internal/persist"
	"realtime/internal/queue"

	"github.com/gorilla/websocket"
)

type Server struct {
	Hub    *Hub
	config config.Config
}

func NewServer(cfg config.Config) *Server {
	var fileWriter *persist.FileWriter
	if cfg.EnablePersist && cfg.PersistPath != "" {
		fw, err := persist.NewFileWriter(cfg.PersistPath, cfg.Logger)
		if err != nil {
			cfg.Logger.Error("Failed to create file writer", "err", err)
		} else {
			fileWriter = fw
			cfg.Logger.Info("File writer initialized", "path", cfg.PersistPath)
		}
	}

	// Tạo Hub với FileWriter
	h := NewHub(cfg.Logger, cfg.Broker, cfg.ShutdownCtx, fileWriter, "server-0001")

	// Gán callbacks (tách biệt Local và Broker)
	h.OnConnect = cfg.OnConnect
	h.OnDisconnect = cfg.OnDisconnect
	h.OnLocalMessage = cfg.OnLocalMessage   // message từ client
	h.OnBrokerMessage = cfg.OnBrokerMessage // message từ Redis

	go h.Run()

	return &Server{
		Hub:    h,
		config: cfg,
	}
}
func (s *Server) Handler() http.HandlerFunc {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	return func(w http.ResponseWriter, r *http.Request) {
		var clientID string
		var metadata map[string]interface{}
		if s.config.AuthFunc != nil {
			id, meta, err := s.config.AuthFunc(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}
			clientID = id
			metadata = meta
		} else {
			clientID = s.config.UID.ToBase58()
			metadata = make(map[string]interface{})
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			s.config.Logger.Error("upgrade error", "err", err)
			return
		}

		cl := client.NewClient("1", clientID, conn, s.Hub, metadata)

		go cl.WritePump()
		go cl.ReadPump()

		cl.WaitReady()

		s.Hub.GetRegister() <- cl
	}
}

// Convenience methods
func (s *Server) Broadcast(msg model.Message) {
	helper.BroadcastToAll(s.Hub, msg)
}

func (s *Server) BroadcastAndBroker(msg model.Message, msgQueue *queue.Queue) {
	broker.BroadcastToAllAndBroker(s.Hub, msg, msgQueue)
}

func (s *Server) SendTo(clientID string, msg model.Message) error {
	return helper.SendToClient(s.Hub, clientID, msg)
}

func (s *Server) GetOnlineClients() []string {
	return helper.GetOnlineClients(s.Hub)
}
