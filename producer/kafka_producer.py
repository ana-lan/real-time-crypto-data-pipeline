import json
import time
import os
import logging
from confluent_kafka import Producer
from dotenv import load_dotenv
from coingecko_client import fetch_crypto_data

load_dotenv('../config/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def delivery_report(err, msg):
    """
    Callback function that Kafka calls after each message is delivered
    (or fails to deliver). This is how you know if publishing succeeded.
    
    err: None if successful, an error object if it failed
    msg: the message object with topic, partition, offset info
    """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        # msg.topic(), msg.partition(), msg.offset() tell you exactly
        # where in Kafka this message landed
        logger.debug(
            f"Delivered to topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )

def create_producer(bootstrap_servers: str) -> Producer:
    """
    Creates and returns a Kafka Producer instance.
    
    The config dict controls producer behavior:
    - bootstrap.servers: where Kafka is running
    - acks: how many broker acknowledgements to wait for before
      considering a message "sent". 'all' = most reliable, '0' = fastest
    - retries: how many times to retry on transient failure
    - retry.backoff.ms: how long to wait between retries
    """
    config = {
        'bootstrap.servers': bootstrap_servers,
        'acks' : 'all',
        'retries' : 3,
        'retry.backoff.ms' : 500
    }
    return Producer(config)

def publish_coin(producer: Producer, topic: str, coin: dict) -> None:
    """
    Publishes a single coin record to Kafka.
    
    Key concepts:
    - We serialize the dict to JSON string because Kafka messages are bytes
    - We use coin['id'] as the message KEY — this ensures all messages
      for the same coin always go to the same partition (key-based routing)
      which preserves ordering per coin
    - on_delivery registers our callback for delivery confirmation
    """
    coin_string = json.dumps(coin)
    producer.produce(
        topic=topic, 
        key=coin['id'].encode('utf-8'),
        value=coin_string.encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.poll(0)
 
def run_producer():
    """
    Main loop — fetches from CoinGecko every FETCH_INTERVAL seconds
    and publishes each coin record to Kafka.
    """
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = os.getenv('KAFKA_TOPIC', 'crypto-prices')
    api_url = os.getenv('COINGECKO_API_URL')
    interval = int(os.getenv('FETCH_INTERVAL_SECONDS', '5'))
    
    producer = create_producer(bootstrap_servers)
    logger.info(f"Producer started. Publishing to topic: {topic} every {interval}s")
    
    try:
        while True:
            coins = fetch_crypto_data(api_url)
            
            if not coins:
                logger.warning("No data fetched this cycle, skipping publish")
                time.sleep(interval)
                continue
            
            published_count = 0
            for coin in coins:
                publish_coin(producer, topic, coin)
                published_count += 1
            
            # flush() blocks until all pending messages are delivered
            # Call it after each batch, not after each message
            producer.flush()
            logger.info(f"Published {published_count} records to Kafka")
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    finally:
        producer.flush()

if __name__ == "__main__":
    run_producer()