package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	KafkaBrokers      []string      // Адреса брокеров Kafka
	KafkaTopic        string        // Имя топика
	ProducerMode      string        // Режим: sync, async, batch
	BatchSize         int           // Размер батча для batch-режима
	BatchTimeout      time.Duration // Таймаут ожидания заполнения батча
	HTTPPort          string        // Порт для метрик и управления
	Mode              string        // Режим генерации: regular, peak, night
	EventsPerSecond   int           // Целевая скорость генерации
}

func Load() *Config {
	// Kafka
	brokers := []string{"localhost:29092"}
	if env := os.Getenv("KAFKA_BROKERS"); env != "" {
		brokers = []string{env}
	}

	topic := "page_views"
	if env := os.Getenv("KAFKA_TOPIC"); env != "" {
		topic = env
	}

	// Producer
	mode := getEnv("PRODUCER_MODE", "async") // sync, async, batch
	batchSize, _ := strconv.Atoi(getEnv("BATCH_SIZE", "100"))
	batchTimeout, _ := time.ParseDuration(getEnv("BATCH_TIMEOUT", "1s"))

	// Генератор
	genMode := getEnv("GEN_MODE", "regular") // regular, peak, night
	eventsPerSec, _ := strconv.Atoi(getEnv("EVENTS_PER_SEC", "5"))

	// HTTP
	httpPort := getEnv("HTTP_PORT", ":8081")

	return &Config{
		KafkaBrokers:      brokers,
		KafkaTopic:        topic,
		ProducerMode:      mode,
		BatchSize:         batchSize,
		BatchTimeout:      batchTimeout,
		HTTPPort:          httpPort,
		Mode:              genMode,
		EventsPerSecond:   eventsPerSec,
	}
}

func getEnv(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}