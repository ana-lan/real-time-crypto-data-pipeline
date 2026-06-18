import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from schema import COIN_SCHEMA
from dotenv import load_dotenv

load_dotenv('../config/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
S3_CLEAN_PATH = f"s3a://{os.getenv('S3_BUCKET')}/crypto/clean/"
S3_DLQ_PATH = f"s3a://{os.getenv('S3_BUCKET')}/crypto/dead_letter/"
CHECKPOINT_PATH = "/tmp/spark_checkpoints/crypto"
# We use /tmp for checkpoints locally — in production this would be S3
# replace YOUR-BUCKET-NAME with your actual S3 bucket name

# ── Spark Session ──────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession — the entry point to all Spark
    functionality. Think of it like a database connection but for Spark.
    
    The config options here tell Spark:
    - Which packages to download (Kafka connector, AWS/S3 connector)
    - How to authenticate with AWS S3
    - Which S3 filesystem implementation to use (s3a is the modern one)
    """
    return (
        SparkSession.builder
        .appName("CryptoStreamingJob")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .getOrCreate()
    )


# ── Read Stream from Kafka ─────────────────────────────────────────
def read_kafka_stream(spark: SparkSession):
    """
    Connects to Kafka and returns a streaming DataFrame.
    
    What comes out of Kafka is a DataFrame with these fixed columns:
    - key: binary (the coin ID we set as the key)
    - value: binary (the JSON bytes of the coin record)
    - topic: string
    - partition: integer
    - offset: long
    - timestamp: timestamp
    
    We only care about 'value' — that's our JSON payload.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

# ── Parse JSON payload ─────────────────────────────────────────────
def parse_stream(raw_df):
    """
    The raw Kafka DataFrame has a binary 'value' column.
    This function:
    1. Casts 'value' from binary to string
    2. Parses the JSON string into typed columns using our schema
    3. Returns a flat DataFrame with one column per field
    
    F.from_json() is the key function here — it takes a JSON string
    column and a schema, and expands it into structured columns.
    """
    step1_df = raw_df.select(
        F.col("value").cast(StringType()).alias("json_str"),
        F.col("timestamp").alias("kafka_timestamp")
    )
    step2_df = step1_df.withColumn(
        "data", F.from_json(F.col("json_str"), COIN_SCHEMA)
    )
    parsed_df = step2_df.select("data.*", "kafka_timestamp")
    return parsed_df

# ── Validate records ───────────────────────────────────────────────
def split_clean_and_dlq(parsed_df):
    """
    Splits the parsed DataFrame into two:
    - clean_df: records that pass all validation rules
    - dlq_df: records that fail any validation rule
    
    Validation rules:
    - id is not null
    - current_price is not null AND > 0
    - market_cap is not null AND > 0
    - fetch_timestamp is not null
    
    This is where dead letter queue routing happens. Bad records
    don't get dropped — they get saved separately so you can
    investigate what went wrong with upstream data.
    """
    is_valid = (
        F.col("id").isNotNull() &
        (F.col("current_price").isNotNull()) & 
        (F.col("current_price") > 0) &
        (F.col("market_cap").isNotNull()) & 
        (F.col("market_cap") > 0) &
        F.col("fetch_timestamp").isNotNull()
    )
    clean_df = parsed_df.filter(is_valid)
    dlq_df = parsed_df.filter(~is_valid)

    return clean_df, dlq_df


# ── Add partition columns ──────────────────────────────────────────
def add_partition_columns(df):
    """
    Adds date and hour columns derived from fetch_timestamp.
    These become the S3 folder structure:
    s3a://bucket/crypto/clean/coin=bitcoin/date=2025-05-01/hour=14/
    
    Why partition this way?
    - Athena can skip entire folders when your WHERE clause filters
      by coin, date, or hour
    - This is what gives you the 40% query latency improvement
    """
    df_with_ts = df.withColumn("fetch_ts", F.to_timestamp(F.col("fetch_timestamp")))
    df_with_date = df_with_ts.withColumn("date", F.to_date(F.col("fetch_ts")))
    df_with_hour = df_with_date.withColumn("hour", F.hour(F.col("fetch_ts")))
    return df_with_hour


# ── Write micro-batch ──────────────────────────────────────────────
def process_batch(batch_df, batch_id):
    """
    This function is called by Spark once per micro-batch.
    batch_df is a regular static DataFrame — not a stream.
    batch_id is an incrementing integer identifying which batch this is.
    
    We:
    1. Skip empty batches
    2. Parse and validate
    3. Write clean records to S3 partitioned by coin/date/hour
    4. Write bad records to S3 dead letter prefix
    """
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: empty, skipping")
        return

    logger.info(f"Batch {batch_id}: processing {batch_df.count()} records")
    
    parsed_df = parse_stream(batch_df)
    clean_df, dlq_df = split_clean_and_dlq(parsed_df)
    clean_df = add_partition_columns(clean_df)

    try:
        clean_count = clean_df.count()
        (
            clean_df.write
            .mode("append")
            .partitionBy("id", "date", "hour")
            .parquet(S3_CLEAN_PATH)
        )
        logger.info(f"Batch {batch_id}: wrote {clean_count} clean records")
    except Exception as e:
        logger.error(f"Batch {batch_id}: failed to write clean records - {e}")
    
    try:
        dlq_count = dlq_df.count()
        (
            dlq_df.write
            .mode("append")
            .parquet(S3_DLQ_PATH)
        )
        logger.info(f"Batch {batch_id}: wrote {dlq_count} dead letter records")
    except Exception as e:
        logger.error(f"Batch {batch_id}: failed to write DLQ records - {e}")


# ── Main ───────────────────────────────────────────────────────────
def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    # setLogLevel WARN reduces Spark's own verbose logs so you can
    # see your logger.info messages clearly

    raw_df = read_kafka_stream(spark)

    query = (
        raw_df.writeStream
        .foreachBatch(process_batch)
        # foreachBatch passes each micro-batch to our process_batch function
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        # Process a new micro-batch every 10 seconds
        .start()
    )

    logger.info("Streaming job started. Waiting for data...")
    query.awaitTermination()
    # Blocks here and keeps the job running until you Ctrl+C


if __name__ == "__main__":
    run()