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


# ============================================================================
# Configuration Constants
# ============================================================================

class ConsumptionPatterns:
    """Residential electricity consumption patterns (watts)"""
    # Peak hours: morning (6-9am) and evening (5-10pm)
    PEAK_MIN = 2000
    PEAK_MAX = 5000

    # Night hours: 10pm-6am (baseline loads)
    NIGHT_MIN = 500
    NIGHT_MAX = 1500

    # Day hours: 9am-5pm (moderate usage)
    DAY_MIN = 1000
    DAY_MAX = 3000

    # Randomness factor (±20%)
    VARIATION_MIN = 0.8
    VARIATION_MAX = 1.2


class SolarPatterns:
    """Solar production patterns (watts)"""
    DAYLIGHT_START = 6
    DAYLIGHT_END = 18
    PEAK_HOUR = 12

    # Peak production capacity
    PRODUCTION_MIN = 3000
    PRODUCTION_MAX = 6000

    # Randomness factor (±20%)
    VARIATION_MIN = 0.8
    VARIATION_MAX = 1.2


class DataQuality:
    """Reading status distribution"""
    # Status weights: 98% valid, 1.5% estimated, 0.5% error
    VALID_THRESHOLD = 98.0
    ESTIMATED_THRESHOLD = 99.5  # 98.0 + 1.5
    # Above 99.5 = error

    # Status codes (optimized CHAR(1) storage)
    STATUS_VALID = 'V'
    STATUS_ESTIMATED = 'E'
    STATUS_ERROR = 'R'


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
                'retention.ms': '604800000',   # 7 days retention
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
    """Simulates realistic smart meter readings with cumulative energy values"""

    def __init__(self, meter_count: int, reading_interval_minutes: int):
        self.meter_count = meter_count
        self.reading_interval_minutes = reading_interval_minutes
        self.reading_interval_hours = reading_interval_minutes / 60.0

        # Store cumulative readings for each meter
        # Starts at 0 when producer starts
        self.cumulative_consumption_mwh = {}  # meter_id -> cumulative milliwatt-hours
        self.cumulative_production_mwh = {}   # meter_id -> cumulative milliwatt-hours

        logger.info(f"Initialized simulator for {meter_count:,} meters")
        logger.info(f"Reading interval: {reading_interval_minutes} minutes ({self.reading_interval_hours:.3f} hours)")

    def generate_reading(self, meter_id: int, timestamp: Optional[datetime] = None) -> Dict:
        """
        Generate a realistic cumulative meter reading

        Args:
            meter_id: Unique meter identifier
            timestamp: Reading timestamp (defaults to now)

        Returns:
            Dictionary with cumulative meter reading data in milliwatt-hours
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        # Initialize cumulative readings for this meter if first time
        if meter_id not in self.cumulative_consumption_mwh:
            self.cumulative_consumption_mwh[meter_id] = 0
            self.cumulative_production_mwh[meter_id] = 0

        # Simulate INSTANTANEOUS POWER consumption based on time of day
        hour = timestamp.hour

        # Base consumption varies by time of day (residential pattern)
        if 6 <= hour < 9 or 17 <= hour < 22:
            # Peak hours: morning and evening
            base_consumption_watts = random.uniform(
                ConsumptionPatterns.PEAK_MIN,
                ConsumptionPatterns.PEAK_MAX
            )
        elif 22 <= hour or hour < 6:
            # Night: baseline loads
            base_consumption_watts = random.uniform(
                ConsumptionPatterns.NIGHT_MIN,
                ConsumptionPatterns.NIGHT_MAX
            )
        else:
            # Day: moderate usage
            base_consumption_watts = random.uniform(
                ConsumptionPatterns.DAY_MIN,
                ConsumptionPatterns.DAY_MAX
            )

        # Add randomness (±20%)
        consumption_watts = base_consumption_watts * random.uniform(
            ConsumptionPatterns.VARIATION_MIN,
            ConsumptionPatterns.VARIATION_MAX
        )
        consumption_milliwatts = consumption_watts * 1000

        # Calculate ENERGY consumed during this period: Energy = Power × Time
        # Convert instantaneous power to energy over the reading interval
        energy_consumption_mwh = consumption_milliwatts * self.reading_interval_hours

        # Add to cumulative total
        self.cumulative_consumption_mwh[meter_id] += energy_consumption_mwh

        # 50% of meters have solar panels (production)
        has_solar = (meter_id % 2) == 0
        cumulative_production_mwh = None

        if has_solar:
            # Solar production varies by time of day
            if SolarPatterns.DAYLIGHT_START <= hour < SolarPatterns.DAYLIGHT_END:
                # Peak solar around noon
                solar_factor = 1.0 - abs(hour - SolarPatterns.PEAK_HOUR) / 6
                peak_production = random.uniform(
                    SolarPatterns.PRODUCTION_MIN,
                    SolarPatterns.PRODUCTION_MAX
                )
                production_watts = peak_production * solar_factor * random.uniform(
                    SolarPatterns.VARIATION_MIN,
                    SolarPatterns.VARIATION_MAX
                )
                production_milliwatts = production_watts * 1000
            else:
                production_milliwatts = 0

            # Calculate energy produced during this period
            energy_production_mwh = production_milliwatts * self.reading_interval_hours

            # Add to cumulative total
            self.cumulative_production_mwh[meter_id] += energy_production_mwh
            cumulative_production_mwh = int(self.cumulative_production_mwh[meter_id])

        # Data quality simulation: 98% valid, 1.5% estimated, 0.5% error
        # Optimized: use single random() call instead of choices()
        rand = random.random() * 100
        if rand < DataQuality.VALID_THRESHOLD:
            reading_status = DataQuality.STATUS_VALID
        elif rand < DataQuality.ESTIMATED_THRESHOLD:
            reading_status = DataQuality.STATUS_ESTIMATED
        else:
            reading_status = DataQuality.STATUS_ERROR

        # Return CUMULATIVE readings (total since meter installation/reset)
        reading = {
            'meter_id': meter_id,
            'reading_timestamp': timestamp.isoformat(),
            'reading_consumption_milliwatts': int(self.cumulative_consumption_mwh[meter_id]),
            'reading_production_milliwatts': cumulative_production_mwh,
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
    """Main execution - meter reading generation"""
    # Configuration from environment variables
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'meter_readings')
    METER_COUNT = int(os.getenv('METER_COUNT', '1000'))
    READING_INTERVAL = int(os.getenv('READING_INTERVAL_MINUTES', '15'))
    CONTINUOUS_FLOW = os.getenv('CONTINUOUS_FLOW', 'false').lower() == 'true'

    logger.info("=" * 80)
    logger.info("Smart Meter Producer")
    logger.info("=" * 80)
    logger.info(f"Meters: {METER_COUNT:,} | Interval: {READING_INTERVAL}min")
    logger.info(f"Mode: {'Continuous flow' if CONTINUOUS_FLOW else 'Batch (send all at once)'}")
    logger.info("Features: Realistic consumption patterns, 50% solar penetration")
    logger.info("=" * 80)

    # Step 1: Ensure topic exists with proper configuration
    logger.info("\n[1/3] Checking Kafka topic...")
    topic_manager = KafkaTopicManager(KAFKA_BOOTSTRAP_SERVERS)
    topic_manager.create_topic(KAFKA_TOPIC, num_partitions=4, replication_factor=1)

    # Step 2: Initialize components
    logger.info("\n[2/3] Initializing components...")
    simulator = MeterReadingSimulator(METER_COUNT, READING_INTERVAL)
    producer = MeterDataProducer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    # Step 3: Generate and send readings
    logger.info(f"\n[3/3] Starting meter reading simulation...")
    logger.info("Press Ctrl+C to stop\n")

    cycle_count = 0
    total_readings_sent = 0

    # Calculate delay per message for continuous flow
    if CONTINUOUS_FLOW:
        delay_per_message = (READING_INTERVAL * 60.0) / METER_COUNT
        logger.info(f"Continuous flow: {delay_per_message*1000:.3f}ms delay per message\n")

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

                # If continuous flow, add delay between messages
                if CONTINUOUS_FLOW:
                    time.sleep(delay_per_message)

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

            # In batch mode, wait for next interval
            # In continuous flow mode, we already waited during sending
            if not CONTINUOUS_FLOW:
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
