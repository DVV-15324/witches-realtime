package types

import (
	"context"
	"net/http"
	"realtime/internal/model"
	"sync"
)

type IHub interface {
	HandleLocalMessage(client IClient, message []byte)
	//	BroadcastToLocalClients(message []byte)
	//JoinRoom(clientID, room string)
	//LeaveRoom(clientID, room string)
	//BroadcastToRoomLocal(room string, msg []byte)
	//BroadcastToRoomAndBroker(room string, msg []byte)
	//BroadcastToAll(msg model.Message)
	GetClients() map[string]IClient
	//	GetClientCount() int
	//	GetClientByUserID(userID string) IClient
	GetBroker() IBroker
	//CloseAll()
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
	//HandleLocalMessage(client IClient, message []byte)
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
	Info(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
	Debug(msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
}

type IBroker interface {
	Publish(message []byte) error
	Subscribe(ctx context.Context) (<-chan []byte, error)
	Close() error
}

type AuthFunc func(r *http.Request) (clientID string, metadata map[string]interface{}, err error)
