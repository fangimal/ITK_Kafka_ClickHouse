-- Создаем базу данных
CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

-- 1. Сырые данные
CREATE TABLE IF NOT EXISTS page_views_raw
(
    event_date      Date DEFAULT today(),
    event_time      DateTime64(3, 'UTC'),
    page_id         String,
    user_id         String,
    duration_ms     UInt32,
    user_agent      String,
    ip_address      IPv6,
    region          LowCardinality(String),
    is_bounce       UInt8,
    kafka_offset    Int64,
    kafka_partition Int32,
    processed_time  DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (event_date, page_id, user_id)
    TTL event_date + INTERVAL 30 DAY
    SETTINGS index_granularity = 8192;

-- 2. Агрегированные данные (минутные) - AggregatingMergeTree
CREATE TABLE IF NOT EXISTS page_views_agg_minute
(
    window_start    DateTime,
    page_id         String,
    view_count      AggregateFunction(sum, UInt64),
    total_duration  AggregateFunction(sum, UInt64),
    unique_users    AggregateFunction(uniq, String),
    bounce_count    AggregateFunction(sum, UInt8)
    ) ENGINE = AggregatingMergeTree()
    PARTITION BY toYYYYMM(window_start)
    ORDER BY (window_start, page_id)
    TTL window_start + INTERVAL 7 DAY;

-- 3. Агрегированные данные (часовые) - обычная таблица для финальных результатов
CREATE TABLE IF NOT EXISTS page_views_agg_hour
(
    window_start    DateTime,
    page_id         String,
    view_count      UInt64,
    avg_duration    Float32,
    unique_users    UInt64,
    bounce_rate     Float32
) ENGINE = SummingMergeTree()
    ORDER BY (window_start, page_id);

-- 4. Dead Letter Queue для ошибок
CREATE TABLE IF NOT EXISTS processing_errors
(
    error_time      DateTime,
    raw_message     String,
    error_reason    String,
    kafka_offset    Int64,
    kafka_partition Int32
) ENGINE = MergeTree()
    ORDER BY (error_time);

-- === MATERIALIZED VIEWS ===

-- 1. MV для минутной агрегации
-- ВАЖНО: Явное приведение типов через toUInt64()/toUInt8()!
CREATE MATERIALIZED VIEW IF NOT EXISTS page_views_raw_to_minute
TO page_views_agg_minute
AS SELECT
              toStartOfMinute(event_time) AS window_start,
              page_id,
              sumState(toUInt64(1)) AS view_count,              -- ✅ Явно UInt64
              sumState(toUInt64(duration_ms)) AS total_duration, -- ✅ Явно UInt64
              uniqState(user_id) AS unique_users,
              sumState(toUInt8(is_bounce)) AS bounce_count       -- ✅ Явно UInt8
   FROM page_views_raw
   GROUP BY window_start, page_id;

-- 2. MV для часовой агрегации (НАПРЯМУЮ из raw, минуя minute)
-- Используем обычные агрегатные функции, так как SummingMergeTree хранит готовые значения
CREATE MATERIALIZED VIEW IF NOT EXISTS page_views_raw_to_hour
            TO page_views_agg_hour
AS SELECT
       toStartOfHour(event_time) AS window_start,
       page_id,
       toUInt64(count()) AS view_count,
       toFloat32(avg(duration_ms)) AS avg_duration,
       toUInt64(uniq(user_id)) AS unique_users,
       toFloat32(sum(is_bounce) * 100.0 / count()) AS bounce_rate
   FROM page_views_raw
   GROUP BY window_start, page_id;

-- 3. MV для фильтрации ошибок в DLQ
CREATE MATERIALIZED VIEW IF NOT EXISTS errors_mv
TO processing_errors
AS SELECT
              now() AS error_time,
              concat('page_id:', page_id, ', user_id:', user_id, ', duration:', toString(duration_ms)) AS raw_message,
              'validation_error' AS error_reason,
              kafka_offset,
              kafka_partition
   FROM page_views_raw
   WHERE page_id = '' OR duration_ms = 0 OR duration_ms > 2147483647;
