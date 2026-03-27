package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/config"
	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/event"
	httpapi "github.com/fangimal/ITK_Kafka_ClickHouse/internal/http"
	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/kafka"
	"github.com/fangimal/ITK_Kafka_ClickHouse/internal/metrics"
)

func main() {
	// 1. Загрузка конфигурации
	cfg := config.Load()
	log.Printf("Starting Producer with config: %+v", cfg)

	// 2. Инициализация метрик
	m := metrics.NewMetrics()

	// 3. Инициализация HTTP сервера
	httpConfig := &httpapi.Config{
		EventsPerSecond: cfg.EventsPerSecond,
		Mode:            cfg.Mode,
		Pause:           false,
	}
	httpServer := httpapi.NewServer(cfg.HTTPPort, m, httpConfig)

	// Запуск HTTP сервера в горутине
	go func() {
		if err := httpServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// 4. Инициализация Kafka Producer
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// 5. Запуск слушателей ошибок (для Async режима)
	go producer.ListenErrors()
	go producer.ListenSuccesses()

	// 6. Контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received")
		cancel()

		// Остановка HTTP сервера с таймаутом
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := httpServer.Stop(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}
	}()

	// 7. Запуск генерации событий
	log.Println("Starting event generation...")
	startEventGeneration(ctx, producer, cfg, m, httpServer)
}

// startEventGeneration - основной цикл генерации
func startEventGeneration(ctx context.Context, producer *kafka.Producer, cfg *config.Config, m *metrics.Metrics, httpServer *httpapi.Server) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Producer stopping...")
			return
		default:
			// Проверка паузы через HTTP API
			currentConfig := httpServer.GetConfig()
			if currentConfig.Pause {
				time.Sleep(1 * time.Second)
				continue
			}

			// Получение интервала из текущей конфигурации
			interval := getInterval(currentConfig.Mode)

			// 1. Генерируем событие
			var ev event.PageViewEvent

			// 5% ошибок, 10% броунсов, 85% нормальных
			randNum := event.RndIntn(100)
			if randNum < 5 {
				ev = event.GenerateErrorEvent(event.RndIntn(3))
			} else if randNum < 15 {
				ev = event.GenerateBounceEvent()
			} else {
				ev = event.GenerateNormalEvent()
			}

			// 1% дубликатов (отправляем то же событие ещё раз)
			isDuplicate := event.RndIntn(100) < 1

			// 2. Сериализуем в JSON
			startTime := time.Now()
			data, err := ev.ToJSON()
			if err != nil {
				log.Printf("Failed to marshal event: %v", err)
				m.RecordMessage(0, 0, false)
				continue
			}

			// 3. Отправляем в Kafka с ретраями
			err = producer.SendMessageWithRetry(ev.GetPartitionKey(), data, 3)
			latency := time.Since(startTime).Milliseconds()

			if err != nil {
				log.Printf("Failed to send message after retries: %v", err)
				m.RecordMessage(len(data), latency, false)
			} else {
				m.RecordMessage(len(data), latency, true)

				// Отправка дубликата
				if isDuplicate {
					_ = producer.SendMessageWithRetry(ev.GetPartitionKey(), data, 1)
				}
			}

			// 4. Ждём следующий интервал
			time.Sleep(interval)
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
