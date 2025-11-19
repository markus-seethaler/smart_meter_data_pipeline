#!/usr/bin/env python3
"""
Smart Meter Data Producer - Simulates electricity meter readings

Generates realistic meter readings for 1M+ homes and sends them to Kafka.
Simulates realistic consumption patterns with 50% solar penetration.
"""

import json
import time
import random
import logging
from datetime import datetime, timezone
from typing import Dict, Optional
import os
from dotenv import load_dotenv

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaTopicManager:
    """Manages Kafka topic creation and configuration"""

    def __init__(self, bootstrap_servers: str):
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

    def topic_exists(self, topic_name: str) -> bool:
        """Check if topic exists"""
        metadata = self.admin_client.list_topics(timeout=10)
        return topic_name in metadata.topics

    def create_topic(self, topic_name: str, num_partitions: int = 4, replication_factor: int = 1):
        """
        Create Kafka topic with proper configuration for high-volume meter data

        Args:
            topic_name: Name of the topic
            num_partitions: Number of partitions (default 4 for parallel processing)
            replication_factor: Replication factor (default 1 for single broker)
        """
        if self.topic_exists(topic_name):
            logger.info(f"Topic '{topic_name}' already exists")
            return

        topic = NewTopic(
            topic=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config={
                'compression.type': 'snappy',  # Fast compression
                'retention.ms': '604800000',   # 7 days retention (matches TimescaleDB)
                'segment.bytes': '1073741824'  # 1GB segments
            }
        )

        try:
            futures = self.admin_client.create_topics([topic])

            # Wait for topic creation
            for topic_name, future in futures.items():
                future.result()  # Block until topic is created
                logger.info(f"✓ Topic '{topic_name}' created successfully")
                logger.info(f"  - Partitions: {num_partitions}")
                logger.info(f"  - Compression: snappy")
                logger.info(f"  - Retention: 7 days")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")
            raise


class MeterReadingSimulator:
    """Simulates realistic smart meter readings"""

    def __init__(self, meter_count: int):
        self.meter_count = meter_count
        logger.info(f"Initialized simulator for {meter_count:,} meters")

    def generate_reading(self, meter_id: int, timestamp: Optional[datetime] = None) -> Dict:
        """
        Generate a realistic meter reading

        Args:
            meter_id: Unique meter identifier
            timestamp: Reading timestamp (defaults to now)

        Returns:
            Dictionary with meter reading data
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        # Simulate consumption based on time of day and meter type
        hour = timestamp.hour

        # Base consumption varies by meter type (residential pattern)
        # Higher during morning (6-9am) and evening (5-10pm)
        if 6 <= hour < 9 or 17 <= hour < 22:
            # Peak hours: 2-5 kW for residential
            base_consumption_watts = random.uniform(2000, 5000)
        elif 22 <= hour or hour < 6:
            # Night: 0.5-1.5 kW (baseline loads)
            base_consumption_watts = random.uniform(500, 1500)
        else:
            # Day: 1-3 kW
            base_consumption_watts = random.uniform(1000, 3000)

        # Add some randomness (+/- 20%)
        consumption_watts = base_consumption_watts * random.uniform(0.8, 1.2)
        consumption_milliwatts = int(consumption_watts * 1000)

        # 50% of meters have solar panels (production) - we're a solar company!
        has_solar = (meter_id % 2) == 0
        production_milliwatts = None

        if has_solar:
            # Solar production varies by time of day
            if 6 <= hour < 18:  # Daylight hours
                # Peak solar around noon
                solar_factor = 1.0 - abs(hour - 12) / 6
                peak_production = random.uniform(3000, 6000)  # 3-6 kW peak
                production_watts = peak_production * solar_factor * random.uniform(0.8, 1.2)
                production_milliwatts = int(production_watts * 1000)
            else:
                production_milliwatts = 0

        # 1% chance of estimated/error readings (data quality)
        reading_status = random.choices(
            ['valid', 'estimated', 'error'],
            weights=[98.0, 1.5, 0.5]
        )[0]

        reading = {
            'meter_id': meter_id,
            'reading_timestamp': timestamp.isoformat(),
            'reading_consumption_milliwatts': consumption_milliwatts,
            'reading_production_milliwatts': production_milliwatts,
            'status': reading_status
        }

        return reading


class MeterDataProducer:
    """Produces meter readings to Kafka"""

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'compression.type': 'snappy',
            'linger.ms': 10,  # Batch messages for efficiency
            'batch.size': 65536,  # 64KB batches
            'acks': 1  # Wait for leader acknowledgment
        })
        self.messages_sent = 0
        self.messages_failed = 0

    def delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err:
            self.messages_failed += 1
            logger.error(f'Message delivery failed: {err}')
        else:
            self.messages_sent += 1
            if self.messages_sent % 10000 == 0:
                logger.info(f'Delivered {self.messages_sent:,} messages')

    def send_reading(self, reading: Dict):
        """Send a meter reading to Kafka"""
        try:
            # Use meter_id as key for partitioning (keeps same meter's readings in order)
            key = str(reading['meter_id']).encode('utf-8')
            value = json.dumps(reading).encode('utf-8')

            # Try to send, handle BufferError if queue is full
            while True:
                try:
                    self.producer.produce(
                        topic=self.topic,
                        key=key,
                        value=value,
                        callback=self.delivery_callback
                    )
                    break  # Success, exit loop
                except BufferError:
                    # Queue is full - poll to send some messages and free up space
                    self.producer.poll(0.1)  # Wait 100ms for messages to be sent

            # Poll frequently to prevent queue from filling up
            self.producer.poll(0)

        except Exception as e:
            logger.error(f"Failed to send reading: {e}")
            self.messages_failed += 1

    def flush(self):
        """Flush any pending messages"""
        logger.info("Flushing remaining messages...")
        self.producer.flush()
        logger.info(f"Total messages sent: {self.messages_sent:,}")
        logger.info(f"Total messages failed: {self.messages_failed:,}")


def main():
    """Main execution - simplified meter reading generation"""
    # Configuration from environment variables
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'meter_readings')
    METER_COUNT = int(os.getenv('METER_COUNT', '1000'))
    READING_INTERVAL = int(os.getenv('READING_INTERVAL_MINUTES', '15'))

    logger.info("=" * 80)
    logger.info("Smart Meter Producer")
    logger.info("=" * 80)
    logger.info(f"Meters: {METER_COUNT:,} | Interval: {READING_INTERVAL}min")
    logger.info("Features: Realistic consumption patterns, 50% solar penetration")
    logger.info("=" * 80)

    # Step 1: Ensure topic exists with proper configuration
    logger.info("\n[1/3] Checking Kafka topic...")
    topic_manager = KafkaTopicManager(KAFKA_BOOTSTRAP_SERVERS)
    topic_manager.create_topic(KAFKA_TOPIC, num_partitions=4, replication_factor=1)

    # Step 2: Initialize components
    logger.info("\n[2/3] Initializing components...")
    simulator = MeterReadingSimulator(METER_COUNT)
    producer = MeterDataProducer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    # Step 3: Generate and send readings
    logger.info(f"\n[3/3] Starting meter reading simulation...")
    logger.info("Press Ctrl+C to stop\n")

    cycle_count = 0
    total_readings_sent = 0

    try:
        while True:
            cycle_count += 1
            batch_start = time.time()
            current_timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0)

            readings_this_cycle = 0

            # Generate reading for each meter
            for meter_id in range(1, METER_COUNT + 1):
                reading = simulator.generate_reading(meter_id, current_timestamp)
                producer.send_reading(reading)
                readings_this_cycle += 1

            # Flush all messages
            producer.flush()

            # Statistics
            batch_duration = time.time() - batch_start
            total_readings_sent += readings_this_cycle
            throughput = readings_this_cycle / batch_duration if batch_duration > 0 else 0

            logger.info(
                f"Cycle #{cycle_count:3d} | "
                f"Sent: {readings_this_cycle:,} readings | "
                f"{throughput:,.0f} msg/sec | "
                f"{batch_duration:.1f}s"
            )

            time.sleep(READING_INTERVAL * 60)

    except KeyboardInterrupt:
        logger.info("\n" + "=" * 80)
        logger.info("Shutting down gracefully...")
        producer.flush()

        # Final statistics
        logger.info(f"Final: {cycle_count} cycles | {total_readings_sent:,} total readings")
        logger.info("Producer stopped cleanly")
        logger.info("=" * 80)


if __name__ == "__main__":
    main()
