package types

type ILogger interface {
	Info(msg string)
	Warn(msg string)
	Error(msg string)
	Debug(msg string)
}

type IClient interface {
	ID() string
	UserID() string
	Send(data []byte) error
	Close() error
}
