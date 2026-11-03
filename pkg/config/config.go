package config

type Config struct {
	ServerID      string
	HTTPAddr      string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisChannel  string
	LogPath       string
}
