package clickhouse

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client обёртка над ClickHouse соединением
type Client struct {
	conn driver.Conn
}

// Config настройки подключения
type Config struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

// NewClient создаёт подключение к ClickHouse
func NewClient(cfg Config) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Проверка подключения
	if err = conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	log.Printf("Connected to ClickHouse at %s:%d", cfg.Host, cfg.Port)

	return &Client{conn: conn}, nil
}

// BatchInsert вставляет пакет событий в page_views_raw
func (c *Client) BatchInsert(ctx context.Context, events []Event) error {
	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO page_views_raw 
		(event_date, event_time, page_id, user_id, duration_ms, 
		 user_agent, ip_address, region, is_bounce, 
		 kafka_offset, kafka_partition, processed_time)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, e := range events {
		err = batch.Append(
			e.EventDate,
			e.EventTime,
			e.PageID,
			e.UserID,
			e.DurationMs,
			e.UserAgent,
			e.IPAddress,
			e.Region,
			e.IsBounce,
			e.KafkaOffset,
			e.KafkaPartition,
			time.Now(),
		)
		if err != nil {
			return fmt.Errorf("failed to append event: %w", err)
		}
	}

	return batch.Send()
}

// InsertError вставляет ошибку в processing_errors
func (c *Client) InsertError(ctx context.Context, rawMsg string, reason string, offset int64, partition int32) error {
	return c.conn.Exec(ctx, `
		INSERT INTO processing_errors 
		(error_time, raw_message, error_reason, kafka_offset, kafka_partition)
		VALUES (?, ?, ?, ?, ?)
	`, time.Now(), rawMsg, reason, offset, partition)
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// Event структура для вставки в ClickHouse
type Event struct {
	EventDate      string
	EventTime      time.Time
	PageID         string
	UserID         string
	DurationMs     uint32
	UserAgent      string
	IPAddress      string
	Region         string
	IsBounce       uint8
	KafkaOffset    int64
	KafkaPartition int32
}
