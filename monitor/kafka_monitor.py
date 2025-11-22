#!/usr/bin/env python3
"""
Kafka Queue Monitor - Lean visibility into Kafka topic and consumer metrics

Monitors:
- Topic message count and rate
- Consumer group lag
- Partition distribution
"""

import os
import time
import logging
from typing import Dict
from dotenv import load_dotenv

from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition, KafkaException

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaMonitor:
    """Monitor Kafka topic and consumer group metrics"""

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id

        # Admin client for metadata
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

        # Consumer for watermark checks
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': f"{group_id}-monitor",
            'enable.auto.commit': False
        })

        # Consumer to check actual consumer group offsets (joins the real group temporarily)
        self.offset_consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,  # Same group as actual consumer
            'enable.auto.commit': False
        })

        # Track previous values for rate calculation
        self.prev_offsets = {}
        self.prev_time = None

    def get_topic_watermarks(self) -> Dict[int, tuple]:
        """Get low and high watermarks for each partition"""
        watermarks = {}

        try:
            # Get topic metadata
            metadata = self.admin_client.list_topics(timeout=10)
            if self.topic not in metadata.topics:
                return watermarks

            topic_metadata = metadata.topics[self.topic]

            for partition_id in topic_metadata.partitions.keys():
                try:
                    _, high = self.consumer.get_watermark_offsets(
                        TopicPartition(self.topic, partition_id),
                        timeout=5
                    )
                    watermarks[partition_id] = (0, high)
                except KafkaException:
                    watermarks[partition_id] = (0, 0)

        except Exception as e:
            logger.error(f"Error getting watermarks: {e}")

        return watermarks

    def get_consumer_offsets(self) -> Dict[int, int]:
        """Get current consumer group offsets for each partition"""
        offsets = {}

        try:
            # Get committed offsets for the consumer group
            metadata = self.admin_client.list_topics(timeout=10)
            if self.topic not in metadata.topics:
                return offsets

            topic_metadata = metadata.topics[self.topic]
            partitions = [
                TopicPartition(self.topic, partition_id)
                for partition_id in topic_metadata.partitions.keys()
            ]

            # Get committed offsets using consumer in the same group
            committed = self.offset_consumer.committed(partitions, timeout=5)

            for partition in committed:
                if partition.offset >= 0:
                    offsets[partition.partition] = partition.offset
                else:
                    offsets[partition.partition] = 0

        except Exception as e:
            logger.error(f"Error getting consumer offsets: {e}")

        return offsets

    def calculate_metrics(self) -> Dict:
        """Calculate all metrics"""
        watermarks = self.get_topic_watermarks()
        consumer_offsets = self.get_consumer_offsets()

        # Calculate totals
        total_messages = sum(high for low, high in watermarks.values())
        total_consumed = sum(consumer_offsets.values())
        total_lag = sum(
            watermarks.get(partition, (0, 0))[1] - offset
            for partition, offset in consumer_offsets.items()
        )

        # Update previous values
        self.prev_offsets = watermarks

        return {
            'total_messages': total_messages,
            'total_consumed': total_consumed,
            'total_lag': total_lag,
            'partition_count': len(watermarks),
            'watermarks': watermarks,
            'consumer_offsets': consumer_offsets
        }

    def monitor(self, interval: int = 10):
        """Main monitoring loop"""
        logger.info("=" * 80)
        logger.info("Kafka Queue Monitor")
        logger.info("=" * 80)
        logger.info(f"Topic: {self.topic} | Consumer Group: {self.group_id}")
        logger.info(f"Reporting interval: {interval} seconds")
        logger.info("=" * 80)

        try:
            while True:
                metrics = self.calculate_metrics()

                # Calculate lag percentage
                lag_pct = (metrics['total_lag'] / metrics['total_messages'] * 100) \
                    if metrics['total_messages'] > 0 else 0

                # Simple one-line log
                logger.info(
                    f"Queue: {metrics['total_messages']:,} msgs | "
                    f"Consumed: {metrics['total_consumed']:,} | "
                    f"Lag: {metrics['total_lag']:,} ({lag_pct:.1f}%) | "
                    f"Partitions: {metrics['partition_count']}"
                )

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("\n" + "=" * 80)
            logger.info("Monitor stopped")
            logger.info("=" * 80)
        finally:
            self.consumer.close()
            self.offset_consumer.close()


def main():
    """Main execution"""
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'meter_readings')
    CONSUMER_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'meter-consumer-group')
    MONITOR_INTERVAL = int(os.getenv('MONITOR_INTERVAL', '10'))  # seconds

    monitor = KafkaMonitor(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, CONSUMER_GROUP_ID)
    monitor.monitor(interval=MONITOR_INTERVAL)


if __name__ == "__main__":
    main()
