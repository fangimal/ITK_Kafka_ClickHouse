package main

import (
	"log"
	"time"

	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/config"
	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/event"
	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/kafka"
)

func main() {
	// 1. Загрузка конфигурации
	cfg := config.Load()
	log.Printf("Starting Producer with config: %+v", cfg)

	// 2. Инициализация Kafka Producer
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// 3. Запуск слушателей ошибок (для Async режима)
	go producer.ListenErrors()
	go producer.ListenSuccesses()

	// 4. Запуск генерации событий
	log.Println("Starting event generation...")
	startEventGeneration(producer, cfg)
}

// startEventGeneration - основной цикл генерации
func startEventGeneration(producer *kafka.Producer, cfg *config.Config) {
	ticker := time.NewTicker(getInterval(cfg.Mode))
	defer ticker.Stop()

	for range ticker.C {
		// 1. Генерируем событие
		var ev event.PageViewEvent

		// 5% ошибок, 10% броунсов, 85% нормальных
		randNum := event.RndIntn(100)
		if randNum < 5 {
			// Ошибочное событие
			ev = event.GenerateErrorEvent(event.RndIntn(3))
		} else if randNum < 15 {
			// Броунс
			ev = event.GenerateBounceEvent()
		} else {
			// Нормальное
			ev = event.GenerateNormalEvent()
		}

		// 2. Сериализуем в JSON
		data, err := ev.ToJSON()
		if err != nil {
			log.Printf("Failed to marshal event: %v", err)
			continue
		}

		// 3. Отправляем в Kafka
		// Ключ партиционирования = page_id
		err = producer.SendMessage(ev.GetPartitionKey(), data)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			// Здесь можно добавить логику Retry с Exponential Backoff
		}
	}
}

// getInterval возвращает интервал генерации в зависимости от режима
func getInterval(mode string) time.Duration {
	switch mode {
	case "peak":
		return 10 * time.Millisecond // 100 событий в секунду
	case "night":
		return 10 * time.Second // 1 событие в 10 секунд
	default: // regular
		return 500 * time.Millisecond // 2 события в секунду
	}
}
