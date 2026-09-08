package persist

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"time"
)

type FileWriter struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
	path   string
	logger interface {
		Info(msg string, args ...interface{})
		Error(msg string, args ...interface{})
	}
}

type MessageLog struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Action    string          `json:"action"`
	Channel   string          `json:"channel"`
	Data      json.RawMessage `json:"data"`
	SenderID  string          `json:"sender_id"`
	Timestamp int64           `json:"timestamp"`
	ServerID  string          `json:"server_id"`
}

func NewFileWriter(path string, logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}) (*FileWriter, error) {
	// Tạo thư mục nếu chưa tồn tại
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	// Tên file theo ngày
	filename := path + "messages-" + time.Now().Format("2006-01-02") + ".log"

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &FileWriter{
		file:   file,
		writer: bufio.NewWriterSize(file, 8192), // Buffer 8KB
		path:   path,
		logger: logger,
	}, nil
}

// Write ghi message vào file
func (fw *FileWriter) Write(message []byte) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	// Ghi message + newline
	if _, err := fw.writer.Write(message); err != nil {
		return err
	}
	if err := fw.writer.WriteByte('\n'); err != nil {
		return err
	}

	// Flush để đảm bảo ghi xuống disk
	return fw.writer.Flush()
}

// WriteMessage ghi message với metadata
func (fw *FileWriter) WriteMessage(msg *MessageLog) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return fw.Write(data)
}

// Rotate file theo ngày
func (fw *FileWriter) Rotate() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	// Flush dữ liệu cũ
	if err := fw.writer.Flush(); err != nil {
		return err
	}
	if err := fw.file.Close(); err != nil {
		return err
	}

	// Tạo file mới với ngày hiện tại
	filename := fw.path + "messages-" + time.Now().Format("2006-01-02") + ".log"
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	fw.file = file
	fw.writer = bufio.NewWriterSize(file, 8192)
	return nil
}

func (fw *FileWriter) Close() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if err := fw.writer.Flush(); err != nil {
		return err
	}
	return fw.file.Close()
}
