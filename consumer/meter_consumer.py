#!/usr/bin/env python3
"""
Smart Meter Data Consumer - Reads meter readings from Kafka and writes to TimescaleDB
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional
from dotenv import load_dotenv

from confluent_kafka import Consumer, KafkaError
import psycopg

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration Constants
# ============================================================================

class KafkaConfig:
    """Kafka consumer configuration constants"""
    MAX_POLL_INTERVAL_MS = 300000  # 5 minutes
    SESSION_TIMEOUT_MS = 60000     # 1 minute
    POLL_TIMEOUT_SEC = 1.0         # Poll timeout for consumer


class BatchConfig:
    """Batch processing configuration"""
    DEFAULT_BATCH_SIZE = 1000
    FLUSH_IDLE_SECONDS = 5         # Flush partial batch after idle time
    PERIODIC_COMMIT_SECONDS = 10   # Commit offset periodically


class LoggingConfig:
    """Logging frequency configuration"""
    BATCH_LOG_FREQUENCY = 10       # Log every N batches
    BATCH_LOG_INTERVAL = 100       # Log every N batches (detailed stats)
    TIME_LOG_FREQUENCY_SEC = 10    # Log every N seconds
    IDLE_STATUS_LOG_SEC = 30       # Log idle status every N seconds


class DataValidation:
    """Data validation constants"""
    REQUIRED_FIELDS = ['meter_id', 'reading_timestamp']
    DEFAULT_STATUS = 'V'           # Valid status (CHAR(1))


class DatabaseWriter:
    """Handles batch writing of meter readings to TimescaleDB"""

    def __init__(self, connection_string: str, batch_size: int = 1000):
        self.connection_string = connection_string
        self.batch_size = batch_size
        self.connection = None
        self.records_written = 0
        self.batches_written = 0
        self.connect()

    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg.connect(self.connection_string)
            self.connection.autocommit = False  # Use transactions for batch inserts
            logger.info("✓ Connected to TimescaleDB")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def write_batch(self, readings: List[Dict]) -> int:
        """
        Write a batch of readings to the database using executemany() with ON CONFLICT

        Uses executemany() for batched inserts with ON CONFLICT DO NOTHING to handle
        duplicate readings gracefully. This is important for at-least-once delivery
        semantics from Kafka (handles consumer crashes before offset commit).

        Performance: 3-5x faster than row-by-row execute() while maintaining safety.

        Args:
            readings: List of meter reading dictionaries

        Returns:
            Number of records successfully written
        """
        if not readings:
            return 0

        try:
            with self.connection.cursor() as cursor:
                # Use INSERT with ON CONFLICT for idempotent inserts
                insert_query = """
                    INSERT INTO raw_meter_readings (
                        reading_timestamp,
                        meter_id,
                        reading_consumption_milliwatts,
                        reading_production_milliwatts,
                        status,
                        arrived_at
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (reading_timestamp, meter_id) DO NOTHING
                """

                arrived_at = datetime.now(timezone.utc)

                # Prepare batch data for executemany()
                data = [
                    (
                        reading['reading_timestamp'],
                        reading['meter_id'],
                        reading.get('reading_consumption_milliwatts'),
                        reading.get('reading_production_milliwatts'),
                        reading.get('status', DataValidation.DEFAULT_STATUS),
                        arrived_at
                    )
                    for reading in readings
                ]

                # Execute batch insert
                cursor.executemany(insert_query, data)

                # Commit transaction
                self.connection.commit()

                count = len(readings)
                self.records_written += count
                self.batches_written += 1

                if self.batches_written % LoggingConfig.BATCH_LOG_FREQUENCY == 0:
                    logger.info(f"Written {self.records_written:,} records ({self.batches_written} batches)")

                return count

        except Exception as e:
            logger.error(f"Database write error: {e}")
            self.connection.rollback()
            raise

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info(f"Database closed. Total records written: {self.records_written:,}")


class MeterReadingConsumer:
    """Consumes meter readings from Kafka and writes to database"""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        db_writer: DatabaseWriter,
        batch_size: int = 1000
    ):
        self.topic = topic
        self.db_writer = db_writer
        self.batch_size = batch_size
        self.running = True
        self.messages_consumed = 0
        self.messages_processed = 0
        self.messages_failed = 0

        # Performance tracking
        self.total_batches = 0
        self.total_db_time = 0.0
        self.start_time = None
        self.last_log_time = None
        self.last_log_batch = 0

        # Configure Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # Start from beginning for first run
            'enable.auto.commit': False,  # Manual commit after successful DB write
            'max.poll.interval.ms': KafkaConfig.MAX_POLL_INTERVAL_MS,
            'session.timeout.ms': KafkaConfig.SESSION_TIMEOUT_MS,
        })

        # Subscribe to topic
        self.consumer.subscribe([topic])
        logger.info(f"✓ Subscribed to Kafka topic: {topic}")

    def process_message(self, message) -> Optional[Dict]:
        """
        Parse and validate a Kafka message

        Args:
            message: Kafka message

        Returns:
            Parsed reading dictionary or None if invalid
        """
        try:
            # Parse JSON
            reading = json.loads(message.value().decode('utf-8'))

            # Basic validation
            if not all(field in reading for field in DataValidation.REQUIRED_FIELDS):
                logger.warning(f"Invalid message: missing required fields")
                return None

            return reading

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return None
        except Exception as e:
            logger.error(f"Message processing error: {e}")
            return None

    def consume_and_write(self):
        """Main consumption loop with batch processing"""
        logger.info("=" * 80)
        logger.info("Smart Meter Consumer - Batch INSERT with Deduplication")
        logger.info("=" * 80)
        logger.info(f"Batch size: {self.batch_size:,} | Topic: {self.topic} | Group: meter-consumer-group")
        logger.info(f"Logging: Every {LoggingConfig.BATCH_LOG_INTERVAL} batches or {LoggingConfig.TIME_LOG_FREQUENCY_SEC} seconds")
        logger.info("=" * 80)
        logger.info("Waiting for messages...")

        batch = []
        last_commit = datetime.now(timezone.utc)
        self.start_time = datetime.now(timezone.utc)
        last_activity_log = datetime.now(timezone.utc)

        try:
            while self.running:
                # Poll for messages
                msg = self.consumer.poll(timeout=KafkaConfig.POLL_TIMEOUT_SEC)

                if msg is None:
                    # No message - flush any pending batch and log status
                    current_time = datetime.now(timezone.utc)

                    # Flush pending batch if idle
                    if batch and (current_time - last_commit).total_seconds() > BatchConfig.FLUSH_IDLE_SECONDS:
                        logger.info(f"Flushing {len(batch)} pending messages from partial batch...")
                        self._flush_batch(batch)
                        self.consumer.commit(asynchronous=False)
                        batch = []
                        last_commit = current_time

                    # Log activity periodically even when idle
                    if (current_time - last_activity_log).total_seconds() >= LoggingConfig.IDLE_STATUS_LOG_SEC:
                        if self.messages_processed > 0:
                            logger.info(f"Status: {self.messages_processed:,} records processed so far (waiting for more messages...)")
                        last_activity_log = current_time

                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - not an error
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        self.messages_failed += 1
                    continue

                # Process message
                self.messages_consumed += 1
                reading = self.process_message(msg)

                if reading:
                    batch.append(reading)
                    self.messages_processed += 1
                else:
                    self.messages_failed += 1

                # Write batch when it reaches batch_size
                if len(batch) >= self.batch_size:
                    self._flush_batch(batch)
                    self.consumer.commit(asynchronous=False)
                    batch = []
                    last_commit = datetime.now(timezone.utc)

                # Periodic commit to avoid losing partial batches
                if (datetime.now(timezone.utc) - last_commit).total_seconds() > BatchConfig.PERIODIC_COMMIT_SECONDS:
                    if batch:
                        self._flush_batch(batch)
                        self.consumer.commit(asynchronous=False)
                        batch = []
                        last_commit = datetime.now(timezone.utc)

        except KeyboardInterrupt:
            logger.info("\n" + "=" * 80)
            logger.info("Shutdown signal received...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            # Flush remaining messages and commit offset when crashing gracefully
            if batch:
                logger.info(f"Flushing {len(batch)} remaining messages...")
                self._flush_batch(batch)
                #  Must commit offset after flushing, or messages will be re-consumed
                try:
                    self.consumer.commit(asynchronous=False)
                    logger.info("Final offset committed successfully")
                except Exception as e:
                    logger.error(f"Failed to commit final offset: {e}")

            # Close consumer
            self.consumer.close()

            # Final stats
            total_elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds() if self.start_time else 1
            overall_throughput = self.messages_processed / total_elapsed if total_elapsed > 0 else 0

            logger.info("=" * 80)
            logger.info("Consumer Statistics:")
            logger.info(f"  Messages: {self.messages_consumed:,} consumed | {self.messages_processed:,} processed | {self.messages_failed:,} failed")
            logger.info(f"  Batches: {self.total_batches:,} | Overall throughput: {overall_throughput:,.0f} rec/sec")
            logger.info("Consumer stopped cleanly")
            logger.info("=" * 80)

    def _flush_batch(self, batch: List[Dict]):
        """Write batch to database using executemany() with idempotent inserts"""
        if not batch:
            return

        batch_start = time.time()

        try:
            self.db_writer.write_batch(batch)

            # Track performance
            batch_duration = time.time() - batch_start
            self.total_batches += 1
            self.total_db_time += batch_duration

            # Calculate metrics
            batch_size = len(batch)
            batch_throughput = batch_size / batch_duration if batch_duration > 0 else 0

            # Overall metrics
            total_elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds() if self.start_time else 1
            overall_throughput = self.messages_processed / total_elapsed if total_elapsed > 0 else 0

            # Log periodically: every N batches OR every N seconds
            current_time = datetime.now(timezone.utc)
            should_log = False

            if self.last_log_time is None:
                # First batch
                should_log = True
                self.last_log_time = current_time
            elif self.total_batches - self.last_log_batch >= LoggingConfig.BATCH_LOG_INTERVAL:
                # Every N batches
                should_log = True
            elif (current_time - self.last_log_time).total_seconds() >= LoggingConfig.TIME_LOG_FREQUENCY_SEC:
                # Every N seconds
                should_log = True

            if should_log:
                batches_since_last = self.total_batches - self.last_log_batch
                records_since_last = batches_since_last * self.batch_size

                logger.info(
                    f"Batches: {self.total_batches:,} (+{batches_since_last}) | "
                    f"Records: {self.messages_processed:,} (+{records_since_last:,}) | "
                    f"DB: {batch_duration:.2f}s ({batch_throughput:,.0f} rec/sec) | "
                    f"Overall: {overall_throughput:,.0f} rec/sec"
                )

                self.last_log_time = current_time
                self.last_log_batch = self.total_batches

        except Exception as e:
            logger.error(f"Failed to write batch: {e}")
            raise

    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down consumer...")
        self.running = False


def main():
    """Main execution"""
    # Configuration from environment variables
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'meter_readings')
    KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'meter-consumer-group')

    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'timescaledb')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'smart_meter_db')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'meter_admin')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'meter_pass_2024')

    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))

    # Build connection string
    connection_string = (
        f"host={POSTGRES_HOST} "
        f"port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )

    logger.info("=" * 60)
    logger.info("Configuration:")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Topic: {KAFKA_TOPIC}")
    logger.info(f"  Consumer Group: {KAFKA_GROUP_ID}")
    logger.info(f"  Database: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    logger.info(f"  Batch Size: {BATCH_SIZE}")
    logger.info("=" * 60)

    # Initialize database writer
    db_writer = DatabaseWriter(connection_string, batch_size=BATCH_SIZE)

    # Initialize consumer
    consumer = MeterReadingConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
        group_id=KAFKA_GROUP_ID,
        db_writer=db_writer,
        batch_size=BATCH_SIZE
    )

    # Handle graceful shutdown
    def signal_handler(sig, frame):
        consumer.shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start consuming
    try:
        consumer.consume_and_write()
    finally:
        db_writer.close()


if __name__ == "__main__":
    main()
