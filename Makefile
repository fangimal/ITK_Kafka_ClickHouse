.PHONY: up down logs clean sql-client kafka-topics help

GREEN := \033[0;32m
NC := \033[0m # No Color

help:
	@echo "$(GREEN)=== Analytics Learning Platform ===$(NC)"
	@echo "make up          - Поднять все сервисы (сборка + запуск)"
	@echo "make down        - Остановить и удалить контейнеры"
	@echo "make logs        - Показать логи всех сервисов"
	@echo "make sql-client  - Подключиться к консоли ClickHouse"
	@echo "make clean       - Полная очистка (включая тома с данными)"
	@echo "make kafka-topics- Создать топик для событий (если не создан)"

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