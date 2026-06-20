DROP TABLE IF EXISTS fact_trades;
DROP TABLE IF EXISTS dim_coin;
DROP TABLE IF EXISTS dim_time;

-- ── Dimension table: dim_coin ──────────────────────────────────────
-- One row per coin. Contains descriptive attributes about each coin
-- that don't change with every price snapshot.
-- DISTKEY(coin_id): distributes rows across Redshift slices by coin_id
-- so joins between fact_trades and dim_coin happen on the same slice
-- (avoiding expensive cross-node data movement during joins)
CREATE TABLE dim_coin (
    coin_id     VARCHAR(50)     NOT NULL,
    symbol      VARCHAR(50)     NOT NULL,
    name        VARCHAR(100)    NOT NULL,
    market_cap_rank INTEGER,
    PRIMARY KEY(coin_id)
)
DISTKEY(coin_id)
SORTKEY(coin_id);
-- SORTKEY: physically sorts rows on disk by coin_id
-- makes range scans and joins on coin_id faster

-- ── Dimension table: dim_time ──────────────────────────────────────
-- One row per hour. Pure calendar data — no actual price information.
-- Lets you group and filter by any time dimension without
-- storing redundant date arithmetic in every fact row.
CREATE TABLE dim_time(
    time_id     VARCHAR(20)     NOT NULL,
    -- format: YYYY-MM-DD-HH e.g. '2026-06-19-14'
    date        DATE            NOT NULL,
    hour        INTEGER         NOT NULL,
    day_of_week INTEGER         NOT NULL,
    -- 0=Sunday, 1=Monday ... 6=Saturday (Redshift convention)
    month       INTEGER         NOT NULL,
    year        INTEGER         NOT NULL,
    PRIMARY KEY(time_id)
)
DISTSTYLE ALL;
-- DISTSTYLE ALL: copies this entire table to every Redshift slice
-- Makes sense for small dimension tables — every slice has the full
-- dim_time locally, so joins never need cross-node data movement

-- ── Fact table: fact_trades ────────────────────────────────────────
-- One row per price snapshot. This is the large, central table.
-- References both dimension tables via foreign keys.
CREATE TABLE fact_trades(
    trade_id    BIGINT IDENTITY(1, 1),
    -- IDENTITY: auto-incrementing surrogate key, Redshift generates it
    coin_id                     VARCHAR(50)     NOT NULL,
    time_id                     VARCHAR(20)     NOT NULL,
    fetch_timestamp             TIMESTAMP       NOT NULL,
    current_price               FLOAT           NOT NULL,
    market_cap                  BIGINT,
    total_volume                BIGINT,
    high_24h                    FLOAT,
    low_24h                     FLOAT,
    price_change_24h            FLOAT,
    price_change_percentage_24h FLOAT,
    price_change_percentage_1h  FLOAT,
    circulating_supply          FLOAT,
    PRIMARY KEY (trade_id),
    FOREIGN KEY (coin_id) REFERENCES dim_coin(coin_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
)
DISTKEY(coin_id)
-- Same DISTKEY as dim_coin — fact rows and their matching dim_coin
-- rows end up on the same slice, making the join free (no network hop)
SORTKEY(fetch_timestamp);
-- Sort by timestamp so time-range queries scan minimal data