.PHONY: up down logs clean sql-client kafka-topics help

GREEN := \033[0;32m
NC := \033[0m # No Color

help:
	@echo "================================================"
	@echo "  Kafka + ClickHouse Learning Platform    "
	@echo "================================================"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up              - Start all services (Kafka, ClickHouse, UI)"
	@echo "  make down            - Stop containers (keep data)"
	@echo "  make clean           - Full cleanup (including volumes)"
	@echo "  make logs            - Show logs of all services"
	@echo "  make sql-client      - Connect to ClickHouse console"
	@echo "  make kafka-topics    - Create page_views topic"
	@echo ""
	@echo "Go Services:"
	@echo "  make producer        - Start Producer (event generator)"
	@echo "  make consumer        - Start Consumer (aggregator)"
	@echo ""
	@echo "Metrics & Control:"
	@echo "  make metrics         - Get Producer metrics via curl"
	@echo "  make metrics-pretty  - Show PowerShell command for pretty JSON"
	@echo "  make health          - Health check endpoint"
	@echo "  make config-set      - Update config (mode, speed)"
	@echo "  make config-pause    - Pause Producer"
	@echo "  make config-resume   - Resume Producer"
	@echo ""
	@echo "HTTP Endpoints (localhost:8081):"
	@echo "  GET  /metrics        - Stats: sent, failed, latency, uptime"
	@echo "  GET  /config         - Current configuration"
	@echo "  PUT  /config         - Update config (JSON body)"
	@echo "  GET  /health         - Health check"
	@echo ""


up:
	@echo "$(GREEN)Starting infrastructure...$(NC)"
	docker compose up -d --build
	@echo "$(GREEN)Done! Check Kafka UI at http://localhost:8080$(NC)"

down:
	@echo "$(GREEN)Stopping services...$(NC)"
	docker compose down

logs:
	docker compose logs -f

# Подключение к CLI ClickHouse внутри контейнера
sql-client:
	@echo "$(GREEN)Connecting to ClickHouse...$(NC)"
	docker exec -it analytics-clickhouse clickhouse-client --user user --password password --database analytics

# Очистка всего, включая данные (осторожно!)
clean:
	@echo "$(GREEN)Cleaning everything (volumes included)...$(NC)"
	docker compose down -v
	@echo "$(GREEN)Cleanup complete.$(NC)"

# Пример создания топика через консоль Kafka (внутри контейнера)
kafka-topics:
	docker exec -it analytics-kafka kafka-topics --create --topic page_views --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
	@echo "$(GREEN)Topic 'page_views' created.$(NC)"

# Запуск Producer
producer:
	go run ./cmd/producer

# Запуск Consumer
consumer:
	go run ./cmd/consumer

# Проверка метрик (кроссплатформенно)
metrics:
	@echo "Fetching metrics from http://localhost:8081/metrics"
	@curl -s http://localhost:8081/metrics || echo "curl not available, try opening in browser"

# Красивый вывод для PowerShell (отдельная цель)
metrics-pretty:
	@echo "Use in PowerShell:"
	@echo "Invoke-RestMethod http://localhost:8081/metrics | ConvertTo-Json -Depth 10"

# Проверка здоровья
health:
	curl http://localhost:8081/health

# Обновление конфигурации Producer
config-set:
	curl -X PUT http://localhost:8081/config \
		-H "Content-Type: application/json" \
		-d '{"mode":"peak","events_per_second":100,"pause":false}'

# Пауза Producer
config-pause:
	curl -X PUT http://localhost:8081/config \
		-H "Content-Type: application/json" \
		-d '{"pause":true}'

# Возобновление Producer
config-resume:
	curl -X PUT http://localhost:8081/config \
		-H "Content-Type: application/json" \
		-d '{"pause":false}'