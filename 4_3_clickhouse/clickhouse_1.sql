-- 1. Создаем таблицу для сырых событий с TTL 30 дней
CREATE TABLE user_events
(
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

-- 2. Создаем агрегированную таблицу с TTL 180 дней
CREATE TABLE user_events_agg
(
    event_date Date,
    event_type String,
    total_spent AggregateFunction(sum, UInt32),
    unique_users AggregateFunction(uniq, UInt32),
    total_actions AggregateFunction(count, UInt32)
)
ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

-- 3. Materialized View для автоматического обновления агрегированной таблицы
CREATE MATERIALIZED VIEW user_events_mv TO user_events_agg AS
SELECT
    toDate(event_time) AS event_date,
    event_type,
    sumState(points_spent) AS total_spent,
    uniqState(user_id) AS unique_users,
    countState() AS total_actions
FROM user_events
GROUP BY event_date, event_type;

-- 4. Запрос для расчета Retention
SELECT
    event_date AS day_0,
    uniq(user_id) AS total_users_day_0,
    countIf(next_event <= event_date + INTERVAL 7 DAY) AS returned_in_7_days,
    round(returned_in_7_days / total_users_day_0 * 100, 2) AS retention_7d_percent
FROM
(
    SELECT
        user_id,
        toDate(event_time) AS event_date,
        min(event_time) OVER (PARTITION BY user_id ORDER BY event_time ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS next_event
    FROM user_events
    WHERE event_type = 'login'
)
GROUP BY event_date
ORDER BY event_date;

-- 5. Запрос для быстрой аналитики с использованием агрегированной таблицы
SELECT
    event_date,
    event_type,
    uniqMerge(unique_users) AS unique_users,
    sumMerge(total_spent) AS total_spent,
    countMerge(total_actions) AS total_actions
FROM user_events_agg
GROUP BY event_date, event_type
ORDER BY event_date, event_type;

