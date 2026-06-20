-- ── Query 1: LAG — price change between consecutive snapshots ──────
-- LAG looks at the previous row's value within a partition
-- Use case: detect sudden price movements between snapshots
SELECT
    coin_id,
    fetch_timestamp,
    current_price,
    LAG(current_price) OVER (
        PARTITION BY coin_id
        ORDER BY fetch_timestamp
    ) AS prev_price,
    current_price - LAG(current_price) OVER (
        PARTITION BY coin_id
        ORDER BY fetch_timestamp
    ) AS price_delta
FROM fact_trades
ORDER BY coin_id, fetch_timestamp;

-- ── Query 2: RANK — top coins by price change per time period ──────
-- RANK assigns a position within each group
-- Use case: which coins moved most in each hour
SELECT
    t.time_id,
    f.coin_id,
    c.name,
    f.price_change_percentage_24h,
    RANK() OVER (
        PARTITION BY f.time_id
        ORDER BY f.price_change_percentage_24h DESC
    ) AS rank_by_change
FROM fact_trades f
JOIN dim_coin c ON f.coin_id = c.coin_id
JOIN dim_time t ON f.time_id = t.time_id
ORDER BY t.time_id, rank_by_change;

-- ── Query 3: Moving average — smooth price trend ───────────────────
-- AVG over a window of preceding rows
-- Use case: filter out noise to see actual price direction
SELECT
    coin_id,
    fetch_timestamp,
    current_price,
    AVG(current_price) OVER (
        PARTITION BY coin_id
        ORDER BY fetch_timestamp
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS moving_avg_5
    -- Average of current row + 4 rows before it (5-snapshot window)
FROM fact_trades
ORDER BY coin_id, fetch_timestamp;