CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum
FROM DATA CATALOG
DATABASE 'crypto_pipeline_db'
IAM_ROLE 'arn:aws:iam::979075842708:role/CryptoPipelineRedshiftRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

INSERT INTO dim_coin (coin_id, symbol, name, market_cap_rank)
SELECT DISTINCT
    id              AS coin_id,
    symbol,
    name,
    market_cap_rank
FROM spectrum.clean;

INSERT INTO dim_time (time_id, date, hour, day_of_week, month, year)
SELECT DISTINCT
    date || '-' || LPAD(hour::VARCHAR, 2, '0')      AS time_id,
    date::DATE                                       AS date,
    hour::INTEGER                                    AS hour,
    EXTRACT(DOW FROM date::DATE)::INTEGER            AS day_of_week,
    EXTRACT(MONTH FROM date::DATE)::INTEGER          AS month,
    EXTRACT(YEAR FROM date::DATE)::INTEGER           AS year
FROM spectrum.clean;

INSERT INTO fact_trades (
    coin_id, time_id, fetch_timestamp,
    current_price, market_cap, total_volume,
    high_24h, low_24h, price_change_24h,
    price_change_percentage_24h, price_change_percentage_1h,
    circulating_supply
)
SELECT
    id                                                      AS coin_id,
    date || '-' || LPAD(hour::VARCHAR, 2, '0')             AS time_id,
    fetch_ts::TIMESTAMP                                     AS fetch_timestamp,
    current_price,
    market_cap,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    price_change_percentage_1h,
    circulating_supply
FROM spectrum.clean;
