package hub

import "realtime/internal/queue"

var msgQueue *queue.Queue

func SetQueue(q *queue.Queue) {
	msgQueue = q
}

func GetQueue() *queue.Queue {
	return msgQueue
}
