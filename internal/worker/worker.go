package worker

import (
	"realtime/internal/queue"
	"sync"
	"time"
)

type Task func()

// WorkerPool
type WorkerPool struct {
	tasks   chan Task
	wg      sync.WaitGroup
	quit    chan struct{}
	workers int
}

// NewWorkerPool tạo một worker pool với số lượng worker và kích thước buffer cho channel task.
func NewWorkerPool(numWorkers int, bufferSize int) *WorkerPool {
	wp := &WorkerPool{
		tasks:   make(chan Task, bufferSize),
		quit:    make(chan struct{}),
		workers: numWorkers,
	}
	wp.start()
	return wp
}

// start khởi chạy các worker goroutine.
func (wp *WorkerPool) start() {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go func() {
			defer wp.wg.Done()
			for {
				select {
				case task := <-wp.tasks:
					task()
				case <-wp.quit:
					return
				}
			}
		}()
	}
}

// Submit gửi một task vào hàng đợi xử lý (blocking nếu channel đầy).
func (wp *WorkerPool) Submit(task Task) {
	wp.tasks <- task
}

// SubmitNonBlocking gửi task không blocking; trả về false nếu channel đầy.
func (wp *WorkerPool) SubmitNonBlocking(task Task) bool {
	select {
	case wp.tasks <- task:
		return true
	default:
		return false
	}
}

// Stop dừng tất cả worker và chờ chúng kết thúc.
func (wp *WorkerPool) Stop() {
	close(wp.quit)
	wp.wg.Wait()
}

// QueueWorker xử lý các message lấy từ queue bằng một handler được cung cấp.
// Mỗi worker sẽ poll queue với một interval cố định.
type QueueWorker struct {
	q        *queue.Queue
	handler  func(*queue.Message)
	num      int
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewQueueWorker tạo một QueueWorker với số lượng worker và interval poll.
func NewQueueWorker(q *queue.Queue, handler func(*queue.Message), numWorkers int, interval time.Duration) *QueueWorker {
	return &QueueWorker{
		q:        q,
		handler:  handler,
		num:      numWorkers,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start khởi chạy các worker.
func (qw *QueueWorker) Start() {
	for i := 0; i < qw.num; i++ {
		qw.wg.Add(1)
		go qw.work()
	}
}

// work là vòng lặp xử lý của mỗi worker.
func (qw *QueueWorker) work() {
	defer qw.wg.Done()
	ticker := time.NewTicker(qw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-qw.stopCh:
			return
		case <-ticker.C:
			// Pop một message từ queue (non-blocking)
			msg := qw.q.Pop()
			if msg == nil {
				continue
			}
			// Gọi handler để xử lý message
			qw.handler(msg)
		}
	}
}

// Stop dừng tất cả worker và chờ chúng hoàn thành.
func (qw *QueueWorker) Stop() {
	close(qw.stopCh)
	qw.wg.Wait()
}
