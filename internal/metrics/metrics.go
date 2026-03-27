package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics хранит все метрики сервиса
type Metrics struct {
	MessagesSent    int64
	MessagesFailed  int64
	MessagesRetried int64
	BytesSent       int64
	LatencyTotal    int64 // в миллисекундах (сумма)
	LatencyCount    int64
	StartTime       time.Time
	mu              sync.RWMutex
}

// NewMetrics создаёт новые метрики
func NewMetrics() *Metrics {
	return &Metrics{
		StartTime: time.Now(),
	}
}

// RecordMessage записывает метрики отправки сообщения
func (m *Metrics) RecordMessage(bytes int, latencyMs int64, success bool) {
	if success {
		atomic.AddInt64(&m.MessagesSent, 1)
		atomic.AddInt64(&m.BytesSent, int64(bytes))
		atomic.AddInt64(&m.LatencyTotal, latencyMs)
		atomic.AddInt64(&m.LatencyCount, 1)
	} else {
		atomic.AddInt64(&m.MessagesFailed, 1)
	}
}

// RecordRetry записывает попытку ретрая
func (m *Metrics) RecordRetry() {
	atomic.AddInt64(&m.MessagesRetried, 1)
}

// GetStats возвращает текущую статистику
func (m *Metrics) GetStats() map[string]interface{} {
	messagesSent := atomic.LoadInt64(&m.MessagesSent)
	messagesFailed := atomic.LoadInt64(&m.MessagesFailed)
	messagesRetried := atomic.LoadInt64(&m.MessagesRetried)
	bytesSent := atomic.LoadInt64(&m.BytesSent)
	latencyTotal := atomic.LoadInt64(&m.LatencyTotal)
	latencyCount := atomic.LoadInt64(&m.LatencyCount)

	avgLatency := float64(0)
	if latencyCount > 0 {
		avgLatency = float64(latencyTotal) / float64(latencyCount)
	}

	uptime := time.Since(m.StartTime).Round(time.Second)

	return map[string]interface{}{
		"messages_sent":    messagesSent,
		"messages_failed":  messagesFailed,
		"messages_retried": messagesRetried,
		"bytes_sent":       bytesSent,
		"avg_latency_ms":   avgLatency,
		"uptime":           uptime.String(),
		"success_rate":     calculateRate(messagesSent, messagesFailed),
	}
}

func calculateRate(success, failed int64) string {
	total := success + failed
	if total == 0 {
		return "0%"
	}
	return fmt.Sprintf("%.2f%%", float64(success)*100/float64(total))
}
