package config

import (
	"context"
	"realtime/internal/types"
	"time"
)

type Config struct {
	Logger       types.ILogger
	UID          types.IUID
	Broker       types.IBroker
	AuthFunc     types.AuthFunc
	ShutdownCtx  context.Context
	WSPath       string
	PingInterval time.Duration

	// Redis config (optional)
	RedisAddr        string
	RedisPassword    string
	RedisDB          int
	RedisChannel     string
	DisableProcessor bool

	// Persist
	PersistPath   string
	EnablePersist bool

	// Callbacks
	OnConnect       func(client types.IClient)
	OnDisconnect    func(client types.IClient)
	OnLocalMessage  func(client types.IClient, msg []byte)
	OnBrokerMessage func(msg []byte)
}
