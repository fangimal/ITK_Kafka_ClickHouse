package kafka

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/config"
)

// Producer обёртка над sarama.Producer
type Producer struct {
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	config        *config.Config
}

// NewProducer создаёт и настраивает Kafka Producer
func NewProducer(cfg *config.Config) (*Producer, error) {
	// 1. Базовая конфигурация Sarama
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0

	// 2. Настройки подтверждений (Acknowledgements)
	// Producer.Ack = All означает, что лидер и все реплики должны подтвердить запись.
	// Это гарантия At-least-once доставки.
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 3           // Количество попыток при ошибке
	saramaConfig.Producer.Return.Successes = true // Возвращать успешные отправки (для Sync)
	saramaConfig.Producer.Return.Errors = true    // Возвращать ошибки (для Async)

	// 3. Стратегия партиционирования
	// Выбираем стратегию в момент отправки, но по умолчанию ставим Hash
	// Hash использует Key сообщения для выбора партиции (наш случай с page_id)
	saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner

	// 4. Настройки для Batch (если выбран режим batch)
	// Flush.Frequency = 0 означает "ждать пока не наберётся Flush.Bytes или Flush.Messages"
	saramaConfig.Producer.Flush.Messages = cfg.BatchSize
	saramaConfig.Producer.Flush.Frequency = cfg.BatchTimeout

	// 5. Создание клиента
	client, err := sarama.NewClient(cfg.KafkaBrokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// 6. Создаём Sync Producer (для гарантированной доставки)
	syncProducer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create Sync Producer: %w", err)
	}

	// 7. Создаём Async Producer (для высокой производительности)
	asyncProducer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create Async Producer: %w", err)
	}

	log.Printf("Kafka Producer connected to brokers: %v", cfg.KafkaBrokers)

	return &Producer{
		syncProducer:  syncProducer,
		asyncProducer: asyncProducer,
		config:        cfg,
	}, nil
}

// SendMessage отправляет сообщение в Kafka
// Режим выбирается на основе конфигурации
func (p *Producer) SendMessage(key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: p.config.KafkaTopic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	switch p.config.ProducerMode {
	case "sync":
		return p.sendSync(msg)
	case "async":
		return p.sendAsync(msg)
	case "batch":
		// В Sarama batch работает автоматически благодаря настройкам Flush выше
		// Мы просто используем sync producer, но он будет батчить внутри
		return p.sendSync(msg)
	default:
		return p.sendAsync(msg)
	}
}

// sendSync отправляет сообщение синхронно (ждём подтверждения)
func (p *Producer) sendSync(msg *sarama.ProducerMessage) error {
	partition, offset, err := p.syncProducer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("sync send failed: %w", err)
	}
	//логируем успешную отправку
	log.Printf("Message sent: partition=%d, offset=%d", partition, offset)
	return nil
}

// sendAsync отправляет сообщение асинхронно (не ждём подтверждения сразу)
func (p *Producer) sendAsync(msg *sarama.ProducerMessage) error {
	p.asyncProducer.Input() <- msg
	// Ошибки ловим в отдельной горутине (см. ListenErrors)
	return nil
}

// ListenErrors слушает канал ошибок асинхронного продюсера
// Должен быть запущен в отдельной горутине
func (p *Producer) ListenErrors() {
	for err := range p.asyncProducer.Errors() {
		log.Printf("Async Producer Error: %v", err)
	}
}

func (p *Producer) ListenSuccesses() {
	for msg := range p.asyncProducer.Successes() {
		// Можно считать метрики успешных отправок
		_ = msg
	}
}

func (p *Producer) Close() error {
	var err error
	if err = p.syncProducer.Close(); err != nil {
		return err
	}
	if err = p.asyncProducer.Close(); err != nil {
		return err
	}
	return nil
}

// SendMessageWithRetry отправляет сообщение с ретраями и exponential backoff
func (p *Producer) SendMessageWithRetry(key string, value []byte, maxRetries int) error {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := p.SendMessage(key, value)
		if err == nil {
			return nil
		}

		lastErr = err
		log.Printf("Send failed (attempt %d/%d): %v", attempt+1, maxRetries+1, err)

		if attempt < maxRetries {
			// Exponential backoff: 100ms, 200ms, 400ms, 800ms, ...
			backoff := time.Duration(100*(1<<attempt)) * time.Millisecond
			log.Printf("Retrying in %v...", backoff)
			time.Sleep(backoff)
		}
	}

	return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}
