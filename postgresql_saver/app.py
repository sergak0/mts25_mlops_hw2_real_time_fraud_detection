import psycopg2
from confluent_kafka import Consumer, Producer, KafkaError
import logging
from psycopg2 import OperationalError, InterfaceError
import os
import sys
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Set kafka configuration file
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")


class SavingService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([SCORING_TOPIC])

        # Database connection settings
        self.conn = psycopg2.connect(
            dbname="frauddb",
            user="user",
            password="password",
            host="postgres",
            port="5432"
        )
        self.cur = self.conn.cursor()

    def push_records(self, data, attempt=1):
        if attempt == 3:
            logger.error("Failed to push record")
            return None

        try:
            for record in data:
                self.cur.execute(
                    """
                    INSERT INTO transactions (transaction_id, score, fraud_flag)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING;
                    """,
                    (record['transaction_id'], record['score'], record['fraud_flag'])
                )

            self.conn.commit()
        except (OperationalError, InterfaceError) as e:
            logger.info("Connection lost, reconnecting to db:", e)
            self.conn = psycopg2.connect(
                dbname="frauddb",
                user="user",
                password="password",
                host="postgres",
                port="5432"
            )
            self.cur = self.conn.cursor()
            self.push_records(data, attempt=attempt + 1)
        except Exception as e:
            logger.error("Postgress error:", e)

    def save_scores(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                # Десериализация JSON
                data = json.loads(msg.value().decode('utf-8'))
                # logger.info(str(data))
                self.push_records(data)

            except Exception as e:
                logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    logger.info('Starting SavingService scoring service...')
    service = SavingService()
    try:
        service.save_scores()
    except KeyboardInterrupt:
        service.cur.close()
        service.conn.close()
        logger.info('Service stopped by user')
