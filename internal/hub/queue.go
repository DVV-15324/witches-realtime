package hub

import "github.com/DVV-15324/witches-realtime/internal/queue"

var msgQueue *queue.Queue

func SetQueue(q *queue.Queue) {
	msgQueue = q
}

func GetQueue() *queue.Queue {
	return msgQueue
}
