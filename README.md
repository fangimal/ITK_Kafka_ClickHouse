![alt text](image.png)

Задание

Реализовать систему агрегации данных в реальном времени

Стек: 
Go, ClickHouse, Kafka/Rabbit

Разработать 2 сервиса.

1. Генератор событий (Producer)
2. Агрегирующий Consumer


## 1. Генератор событий (Producer)

Требования к генератору:

Структура события
```go
type PageViewEvent struct {
    PageID        string    `json:"page_id"`
    UserID        string    `json:"user_id"`
    ViewDuration  int       `json:"view_duration_ms"`
    Timestamp     time.Time `json:"timestamp"`
    UserAgent     string    `json:"user_agent,omitempty"`
    IPAddress     string    `json:"ip_address,omitempty"`
    Region        string    `json:"region,omitempty"`
    IsBounce      bool      `json:"is_bounce"` // Быстрый уход (<5 сек)
}
```
Задачи:

1.1. Режимы генерации (реализовать все):

- Регулярный поток: Постоянная генерация 1-10 событий в секунду
- Пиковые нагрузки: Периодические "bursts" по 100-1000 событий за 2 секунды
- Ночные периоды: Редкие события (1 в 10 секунд)

1.2. Типы событий:
- Нормальные просмотры: Длительность 10-600 секунд
- Броунсы: Длительность <5 секунд (10% событий)
- Ошибочные события: С преднамеренными ошибками в данных (5%):
     - Пустой page_id
     - Отрицательная длительность
     - Некорректный JSON
1.3. Отрабатываем паттерны работы с брокером:

Должны быть реализованы:
1. Синхронная отправка (для критичных данных)
2. Асинхронная отправка с callback
3. Batch отправка (накопить N сообщений или ждать X времени)
4. Ретри при ошибках с exponential backoff
*5. Разные стратегии партиционирования
   - По ключу (page_id)
   - Round-robin
   - Random

1.4. Дополнительные требования:

Добавить метрики: количество отправленных сообщений, ошибки, latency
Возможность динамического изменения скорости генерации через HTTP-эндпоинт
Запись дубликатов с одинаковым Message Key (1% случаев)

## 2. Агрегирующий Consumer

Требования к анрегатору:

2.1. Паттерны потребления.
Режимы чтения из брокера:
1. Batch consumer: Накопление N сообщений (100-10000) перед обработкой
2. Time-based consumer: Обработка каждые X секунд
3. Hybrid approach: Что наступит раньше (N сообщений или X секунд)
*4. Exactly-once семантика: Использование транзакций Kafka (по желанию)


2.2. Стратегии обработки:

A. Горячий путь (Hot Path):
-- Немедленная агрегация для real-time дашбордов
-- Использовать AggregatingMergeTree для промежуточных состояний

B. Холодный путь (Cold Path):
-- Подробное хранение для детального анализа
-- Использовать TTL для автоматического удаления/агрегации старых данных

2.3. Обработка ошибок:

Dead Letter Queue (DLQ) для некорректных сообщений !!! Обязательно
Ретри с exponential backoff при ошибках ClickHouse
*Компенсирующие транзакции (при откате Kafka транзакции) (по желанию)

2.4. Гарантии доставки:

At-least-once (реализовать обязательно)
Exactly-once (реализовать как продвинутый вариант)
*Ручное управление коммитами offset (по желанию)

## ClickHouse схема

Таблицы:
```sql
-- 1. Сырые данные (для детального анализа)
CREATE TABLE page_views_raw
(
    event_date    Date DEFAULT today(),
    event_time    DateTime64(3, 'UTC'),
    page_id       String,
    user_id       String,
    duration_ms   UInt32,
    user_agent    String,
    ip_address    IPv6,
    region        LowCardinality(String),
    is_bounce     UInt8,
    kafka_offset  Int64,
    kafka_partition Int32,
    processed_time DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, page_id, user_id)
TTL event_date + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 2. Агрегированные данные (минутные срезы)
CREATE TABLE page_views_agg_minute
(
    window_start  DateTime,
    page_id       String,
    view_count    AggregateFunction(sum, UInt64),
    total_duration AggregateFunction(sum, UInt64),
    unique_users  AggregateFunction(uniq, String),
    bounce_count  AggregateFunction(sum, UInt8)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, page_id)
TTL window_start + INTERVAL 7 DAY;

-- 3. Агрегированные данные (часовые срезы - роллинг из минутных)
CREATE TABLE page_views_agg_hour
(
    window_start  DateTime,
    page_id       String,
    view_count    UInt64,
    avg_duration  Float32,
    unique_users  UInt64,
    bounce_rate   Float32
) ENGINE = SummingMergeTree()
ORDER BY (window_start, page_id);

-- 4. Dead Letter Queue для ошибок
CREATE TABLE processing_errors
(
    error_time    DateTime,
    raw_message   String,
    error_reason  String,
    kafka_offset  Int64,
    kafka_partition Int32
) ENGINE = MergeTree()
ORDER BY (error_time);
```

Материализованные представления:
```sql
-- 1. MV для минутной агрегации (из сырых данных)
CREATE MATERIALIZED VIEW page_views_raw_to_minute
TO page_views_agg_minute
AS
SELECT
    toStartOfMinute(event_time) AS window_start,
    page_id,
    sumState(1) as view_count,
    sumState(duration_ms) as total_duration,
    uniqState(user_id) as unique_users,
    sumState(is_bounce) as bounce_count
FROM page_views_raw
GROUP BY window_start, page_id;

-- 2. MV для часовой агрегации (из минутной)
CREATE MATERIALIZED VIEW page_views_minute_to_hour
TO page_views_agg_hour
AS
SELECT
    toStartOfHour(window_start) AS window_start,
    page_id,
    sum(view_count) as view_count,
    sum(total_duration) / sum(view_count) as avg_duration,
    uniq(unique_users) as unique_users,
    sum(bounce_count) * 100.0 / sum(view_count) as bounce_rate
FROM page_views_agg_minute
GROUP BY window_start, page_id;

-- 3. MV для фильтрации ошибок в DLQ
CREATE MATERIALIZED VIEW errors_mv
TO processing_errors
AS
SELECT
    now() as error_time,
    -- Здесь предполагается, что есть поле с исходным сообщением
    raw_message,
    'validation_error' as error_reason,
    kafka_offset,
    kafka_partition
FROM page_views_raw
WHERE page_id = '' OR duration_ms <= 0;

```
