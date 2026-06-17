from pyspark.sql.types import (
    StructType, StructField, 
    StringType, FloatType, LongType, IntegerType
)

COIN_SCHEMA = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("symbol", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("current_price", FloatType(), nullable=True),
    StructField("market_cap", LongType(), nullable=True),
    StructField("market_cap_rank", IntegerType(), nullable=True),
    StructField("total_volume", LongType(), nullable=True),
    StructField("high_24h", FloatType(), nullable=True),
    StructField("low_24h", FloatType(), nullable=True),
    StructField("price_change_24h", FloatType(), nullable=True),
    StructField("price_change_percentage_24h", FloatType(), nullable=True),
    StructField("price_change_percentage_1h", FloatType(), nullable=True),
    StructField("circulating_supply", FloatType(), nullable=True),
    StructField("last_updated", StringType(), nullable=True),
    StructField("fetch_timestamp", StringType(), nullable=True)
])