package types

import (
	"context"
	"github.com/DVV-15324/witches-realtime/internal/model"
	"net/http"
	"sync"
)

type IHub interface {
	HandleLocalMessage(client IClient, message []byte)
	GetClients() map[string]IClient
	GetBroker() IBroker
	GetLogger() ILogger
	GetRegister() chan IClient
	GetMutex() *sync.RWMutex
	GetRooms() map[string]map[string]IClient
	GetUnRegister() chan IClient
	GetBrokerBroadcast() chan []byte
	GetOnConnect() func(IClient)
	GetOnDisconnect() func(IClient)
	GetOnLocalMessage() func(IClient, []byte)
	GetOnBrokerMessage() func([]byte)
}

type IClient interface {
	ReadPump()
	WritePump()
	GetID() string
	GetUserID() string
	GetRoomID() string
	GetTargetID() string
	GetAction() string
	GetMetadata() map[string]interface{}
	Close() error
	WaitReady()
	SendMessage(msg model.Message) error
	SendBytes(data []byte) error
	HandleAction(req *model.Action, raw []byte)
}

type IUID interface {
	ToBase58() string
}

type ILogger interface {
	Info(msg string)
	Warn(msg string)
	Error(msg string)
	Debug(msg string)
}

type IBroker interface {
	Publish(message []byte) error
	Subscribe(ctx context.Context) (<-chan []byte, error)
	Close() error
}

type AuthFunc func(r *http.Request) (clientID string, metadata map[string]interface{}, err error)
