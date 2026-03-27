package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/clickhouse"
	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/config"
	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/event"
	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/kafka"
)

type ClickHouseHandler struct {
	chClient   *clickhouse.Client
	maxRetries int
}

func (h *ClickHouseHandler) Handle(messages []kafka.Message) error {
	var lastErr error

	for attempt := 0; attempt <= h.maxRetries; attempt++ {
		err := h.handleBatch(messages)
		if err == nil {
			return nil
		}

		lastErr = err
		log.Printf("Batch processing failed (attempt %d/%d): %v", attempt+1, h.maxRetries+1, err)

		if attempt < h.maxRetries {
			// Exponential backoff
			backoff := time.Duration(100*(1<<attempt)) * time.Millisecond
			log.Printf("Retrying in %v...", backoff)
			time.Sleep(backoff)
		}
	}

	return lastErr
}

func (h *ClickHouseHandler) handleBatch(messages []kafka.Message) error {
	var events []clickhouse.Event

	for _, msg := range messages {
		var ev event.PageViewEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			// Некорректный JSON - сразу в DLQ
			h.chClient.InsertError(context.Background(), string(msg.Value), "invalid JSON", msg.Offset, msg.Partition)
			continue
		}

		// Валидация
		if err := validateEvent(ev); err != nil {
			h.chClient.InsertError(context.Background(), string(msg.Value), err.Error(), msg.Offset, msg.Partition)
			continue
		}

		events = append(events, toClickHouseEvent(ev, msg))
	}

	if len(events) > 0 {
		return h.chClient.BatchInsert(context.Background(), events)
	}

	return nil
}

func (h *ClickHouseHandler) HandleError(msg kafka.Message, err error) error {
	return h.chClient.InsertError(context.Background(), string(msg.Value), err.Error(), msg.Offset, msg.Partition)
}

// validateEvent проверяет корректность события
func validateEvent(ev event.PageViewEvent) error {
	if ev.PageID == "" {
		return ErrEmptyPageID
	}
	if ev.ViewDuration < 0 {
		return ErrNegativeDuration
	}
	if ev.ViewDuration > 2147483647 {
		return ErrDurationOverflow
	}
	return nil
}

// Ошибки валидации
var (
	ErrEmptyPageID      = &ValidationError{"empty page_id"}
	ErrNegativeDuration = &ValidationError{"negative duration"}
	ErrDurationOverflow = &ValidationError{"duration overflow"}
)

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

// toClickHouseEvent конвертирует событие для ClickHouse
func toClickHouseEvent(ev event.PageViewEvent, msg kafka.Message) clickhouse.Event {
	return clickhouse.Event{
		EventDate:      ev.Timestamp.Format("2006-01-02"),
		EventTime:      ev.Timestamp,
		PageID:         ev.PageID,
		UserID:         ev.UserID,
		DurationMs:     uint32(ev.ViewDuration),
		UserAgent:      ev.UserAgent,
		IPAddress:      ev.IPAddress,
		Region:         ev.Region,
		IsBounce:       boolToUint8(ev.IsBounce),
		KafkaOffset:    msg.Offset,
		KafkaPartition: msg.Partition,
	}
}

func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func main() {
	// Загрузка конфигурации
	cfg := config.Load()

	// Подключение к ClickHouse
	chClient, err := clickhouse.NewClient(clickhouse.Config{
		Host:     "localhost",
		Port:     9000,
		Database: "analytics",
		User:     "user",
		Password: "password",
	})
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer chClient.Close()

	// Создание обработчика
	handler := &ClickHouseHandler{
		chClient:   chClient,
		maxRetries: 3,
	}

	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		Brokers:      cfg.KafkaBrokers,
		Topic:        cfg.KafkaTopic,
		GroupID:      "analytics-consumer-group",
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
	}, handler)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received")
		cancel()
	}()

	// Запуск потребления
	log.Println("Starting Consumer...")
	if err = consumer.Consume(ctx); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
}
