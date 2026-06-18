-- clickhouse/init.sql
-- Выполняется один раз при первом старте контейнера ClickHouse.

CREATE DATABASE IF NOT EXISTS nasa;

CREATE TABLE IF NOT EXISTS nasa.asteroids
(
    -- Идентификаторы
    neo_id                          String,       -- NASA JPL ID (уникальный)
    name                            String,       -- название астероида

    -- Размеры (км)
    diameter_min_km                 Float64,      -- минимальный оценочный диаметр
    diameter_max_km                 Float64,      -- максимальный оценочный диаметр

    -- Признак опасности
    is_potentially_hazardous        UInt8,        -- 1 = опасный, 0 = нет

    -- Данные сближения с Землёй
    close_approach_date             Date,         -- дата сближения (используется в PARTITION BY)
    relative_velocity_kmh           Float64,      -- скорость относительно Земли (км/ч)
    miss_distance_km                Float64,      -- расстояние промаха (км)
    orbiting_body                   String,       -- тело, вокруг которого орбита (обычно Earth)

    -- Служебные поля
    updated_at                      DateTime DEFAULT now(),  -- момент загрузки в CH
    _version                        UInt64                   -- unix timestamp updated_at для ReplacingMergeTree
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(close_approach_date)  -- партиционирование по месяцу сближения
ORDER BY (neo_id, close_approach_date)      -- ключ уникальности: один астероид может сближаться несколько раз
SETTINGS index_granularity = 8192;

-- Пример запроса с дедупликацией:
-- SELECT * FROM nasa.asteroids FINAL WHERE close_approach_date = today();
--
-- Опасные астероиды за период:
-- SELECT name, diameter_max_km, miss_distance_km, relative_velocity_kmh
-- FROM nasa.asteroids FINAL
-- WHERE is_potentially_hazardous = 1
-- AND close_approach_date BETWEEN '2025-05-19' AND today()
-- ORDER BY miss_distance_km ASC;