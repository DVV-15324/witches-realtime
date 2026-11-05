package hub

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/DVV-15324/witches-realtime/pkg/client"
	"github.com/DVV-15324/witches-realtime/pkg/types"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

type Server struct {
	hub      *Hub
	logger   types.ILogger
	upgrader websocket.Upgrader
	rdb      *redis.Client
}

func NewServer(hub *Hub, logger types.ILogger, rdb *redis.Client) *Server {
	return &Server{
		hub:    hub,
		logger: logger,
		rdb:    rdb,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (s *Server) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("user_id")
		if userID == "" {
			http.Error(w, "missing user_id", http.StatusBadRequest)
			return
		}

		roomsStr := r.URL.Query().Get("rooms")
		var rooms []string
		if roomsStr != "" {
			for _, r := range strings.Split(roomsStr, ",") {
				trimmed := strings.TrimSpace(r)
				if trimmed != "" {
					rooms = append(rooms, trimmed)
				}
			}
		}

		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			s.logger.Error(fmt.Sprintf("upgrade: %v", err))
			return
		}

		clientID := fmt.Sprintf("%s-%d", userID, time.Now().UnixNano())

		// Tạo Callback để xử lý khi FE gửi lệnh
		onMessage := func(uid string, msg []byte) {
			s.logger.Info(fmt.Sprintf("Received command from Flutter (%s): %s", uid, string(msg)))

			err := s.rdb.Publish(context.Background(), "mqtt-commands", msg).Err()
			if err != nil {
				s.logger.Error(fmt.Sprintf("Failed to publish command to Redis: %v", err))
			}
		}
		c := client.New(clientID, userID, conn, onMessage, s.logger)
		s.hub.Register(c, rooms)

		go c.WritePump()
		go c.ReadPump(s.hub.Unregister)

		s.logger.Info(fmt.Sprintf("WS connected: user=%s client=%s rooms=%v", userID, clientID, rooms))
	}
}
