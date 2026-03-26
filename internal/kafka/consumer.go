package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Consumer обёртка над Sarama Consumer Group
type Consumer struct {
	group   sarama.ConsumerGroup
	config  *ConsumerConfig
	handler MessageHandler
	mu      sync.Mutex
}

// ConsumerConfig настройки консьюмера
type ConsumerConfig struct {
	Brokers      []string
	Topic        string
	GroupID      string
	BatchSize    int
	BatchTimeout time.Duration
}

// MessageHandler интерфейс для обработки сообщений
type MessageHandler interface {
	Handle(messages []Message) error
	HandleError(msg Message, err error) error
}

// Message представляет сообщение из Kafka
type Message struct {
	Key       string
	Value     []byte
	Offset    int64
	Partition int32
	Topic     string
}

// NewConsumer создаёт нового Consumer
func NewConsumer(cfg *ConsumerConfig, handler MessageHandler) (*Consumer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	log.Printf("Kafka Consumer connected to brokers: %v", cfg.Brokers)

	return &Consumer{
		group:   group,
		config:  cfg,
		handler: handler,
	}, nil
}

// Consume запускает цикл потребления сообщений
func (c *Consumer) Consume(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Println("Consumer stopping...")
			return c.group.Close()
		default:
			// Consume блокирующий, поэтому запускаем в горутине
			if err := c.group.Consume(ctx, []string{c.config.Topic}, c); err != nil {
				log.Printf("Consumer error: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// Setup вызывается при ребалансировке группы
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group rebalanced")
	return nil
}

// Cleanup вызывается при закрытии сессии
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group cleanup")
	return nil
}

// ConsumeClaim обрабатывает сообщения из партиции
// ❗ ИСПРАВЛЕНО: убран ctx из параметров, берём из session.Context()
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context() // ✅ Контекст берём из сессии

	var batch []Message
	timer := time.NewTimer(c.config.BatchTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Обработать остаток батча перед выходом
			if len(batch) > 0 {
				c.processBatch(ctx, session, batch)
			}
			return nil

		case msg := <-claim.Messages():
			batch = append(batch, Message{
				Key:       string(msg.Key),
				Value:     msg.Value,
				Offset:    msg.Offset,
				Partition: msg.Partition,
				Topic:     msg.Topic,
			})

			// Если набрали батч - обрабатываем
			if len(batch) >= c.config.BatchSize {
				c.processBatch(ctx, session, batch)
				batch = nil
				timer.Reset(c.config.BatchTimeout)
			}

		case <-timer.C:
			// Таймаут батча - обрабатываем что есть
			if len(batch) > 0 {
				c.processBatch(ctx, session, batch)
				batch = nil
			}
			timer.Reset(c.config.BatchTimeout)
		}
	}
}

// processBatch обрабатывает пакет сообщений
func (c *Consumer) processBatch(ctx context.Context, session sarama.ConsumerGroupSession, batch []Message) {
	err := c.handler.Handle(batch)
	if err != nil {
		log.Printf("Batch processing error: %v", err)
		// Обработка ошибок для каждого сообщения
		for _, msg := range batch {
			c.handler.HandleError(msg, err)
		}
		// Не коммитим оффсеты при ошибке (At-least-once)
		return
	}

	// Коммит оффсетов только после успешной обработки
	for _, msg := range batch {
		session.MarkOffset(msg.Topic, msg.Partition, msg.Offset+1, "")
	}
}

// Close закрывает Consumer
func (c *Consumer) Close() error {
	return c.group.Close()
}
