#!/usr/bin/env python3
"""
Historical Data Backfill Script

Generates historical meter readings and writes directly to TimescaleDB.
Bypasses Kafka for efficient bulk loading of historical data.

Usage:
    python backfill_historical_data.py --days 2 --meters 1000000 --batch-size 100000
"""

import argparse
import io
import time
import random
import logging
from datetime import datetime, timedelta, timezone
from typing import Tuple

import psycopg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MeterReadingGenerator:
    """Generates realistic meter readings (same logic as producer)"""

    def __init__(self, meter_count: int):
        self.meter_count = meter_count

    def generate_reading(self, meter_id: int, timestamp: datetime) -> Tuple:
        """
        Generate a realistic meter reading

        Returns:
            Tuple: (reading_timestamp, meter_id, consumption_mw, production_mw, status)
        """
        hour = timestamp.hour

        # Base consumption varies by time of day (residential pattern)
        if 6 <= hour < 9 or 17 <= hour < 22:
            # Peak hours: 2-5 kW for residential
            base_consumption_watts = random.uniform(2000, 5000)
        elif 22 <= hour or hour < 6:
            # Night: 0.5-1.5 kW (baseline loads)
            base_consumption_watts = random.uniform(500, 1500)
        else:
            # Day: 1-3 kW
            base_consumption_watts = random.uniform(1000, 3000)

        # Add randomness (+/- 20%)
        consumption_watts = base_consumption_watts * random.uniform(0.8, 1.2)
        consumption_milliwatts = int(consumption_watts * 1000)

        # 50% of meters have solar panels (matches dimension table)
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

        # Data quality distribution (98% valid, 1.5% estimated, 0.5% error)
        reading_status = random.choices(
            ['valid', 'estimated', 'error'],
            weights=[98.0, 1.5, 0.5]
        )[0]

        return (
            timestamp,
            meter_id,
            consumption_milliwatts,
            production_milliwatts,
            reading_status
        )


class HistoricalDataBackfiller:
    """Backfills historical meter readings directly to TimescaleDB"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.generator = None

    def get_database_size(self, conn) -> Tuple[str, int]:
        """Get current database size"""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    pg_size_pretty(pg_database_size(current_database())) as size,
                    pg_database_size(current_database()) as size_bytes
            """)
            result = cur.fetchone()
            return result[0], result[1]

    def get_table_stats(self, conn) -> dict:
        """Get raw_meter_readings table statistics"""
        with conn.cursor() as cur:
            # Get record count and table size
            cur.execute("""
                SELECT
                    COUNT(*) as record_count,
                    pg_size_pretty(pg_total_relation_size('raw_meter_readings')) as total_size,
                    pg_total_relation_size('raw_meter_readings') as total_size_bytes
                FROM raw_meter_readings
            """)
            result = cur.fetchone()

            # Get compression stats
            cur.execute("""
                SELECT
                    COUNT(*) as total_chunks,
                    COUNT(*) FILTER (WHERE is_compressed = true) as compressed_chunks,
                    COUNT(*) FILTER (WHERE is_compressed = false) as uncompressed_chunks
                FROM timescaledb_information.chunks
                WHERE hypertable_name = 'raw_meter_readings'
            """)
            chunk_stats = cur.fetchone()

            return {
                'record_count': result[0],
                'total_size': result[1],
                'total_size_bytes': result[2],
                'total_chunks': chunk_stats[0],
                'compressed_chunks': chunk_stats[1],
                'uncompressed_chunks': chunk_stats[2]
            }


    def backfill(self, days: int, meter_count: int, batch_size: int = 100000):
        """
        Backfill historical data

        Args:
            days: Number of days to backfill
            meter_count: Number of meters
            batch_size: Records to insert per COPY operation
        """
        self.generator = MeterReadingGenerator(meter_count)

        logger.info("=" * 80)
        logger.info("HISTORICAL DATA BACKFILL")
        logger.info("=" * 80)
        logger.info(f"Days to backfill:     {days}")
        logger.info(f"Meters:               {meter_count:,}")
        logger.info(f"Interval:             15 minutes")
        logger.info(f"Batch size:           {batch_size:,} records")

        # Calculate total records
        readings_per_day = meter_count * 4 * 24  # 4 readings per hour * 24 hours
        total_readings = readings_per_day * days
        logger.info(f"Total readings:       {total_readings:,}")
        logger.info("=" * 80)

        try:
            with psycopg.connect(self.connection_string) as conn:
                # Get initial stats
                logger.info("\n📊 Initial database state:")
                initial_size, initial_size_bytes = self.get_database_size(conn)
                initial_stats = self.get_table_stats(conn)
                logger.info(f"  Database size:      {initial_size}")
                logger.info(f"  Existing records:   {initial_stats['record_count']:,}")
                logger.info(f"  Table size:         {initial_stats['total_size']}")
                logger.info(f"  Chunks:             {initial_stats['total_chunks']} " +
                          f"({initial_stats['compressed_chunks']} compressed, " +
                          f"{initial_stats['uncompressed_chunks']} uncompressed)")

                # Generate timestamps (15-minute intervals going backwards from now)
                end_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                start_time = end_time - timedelta(days=days)

                timestamps = []
                current = start_time
                while current <= end_time:
                    timestamps.append(current)
                    current += timedelta(minutes=15)

                total_timestamps = len(timestamps)
                logger.info(f"\n⏰ Time range:")
                logger.info(f"  Start:              {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                logger.info(f"  End:                {end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                logger.info(f"  Timestamps:         {total_timestamps}")

                # Backfill in batches
                logger.info(f"\n🚀 Starting backfill...")
                start_backfill = time.time()
                records_inserted = 0

                buffer = io.StringIO()

                for ts_idx, timestamp in enumerate(timestamps, 1):
                    # Generate readings for all meters at this timestamp
                    for meter_id in range(1, meter_count + 1):
                        reading = self.generator.generate_reading(meter_id, timestamp)

                        # Format for COPY (tab-separated)
                        # Format: timestamp \t meter_id \t consumption \t production \t status \t arrived_at
                        arrived_at = datetime.now(timezone.utc)
                        production = reading[3] if reading[3] is not None else '\\N'  # NULL in COPY format

                        buffer.write(f"{reading[0].isoformat()}\t{reading[1]}\t{reading[2]}\t{production}\t{reading[4]}\t{arrived_at.isoformat()}\n")
                        records_inserted += 1

                        # Flush buffer when batch size reached
                        if records_inserted % batch_size == 0:
                            self._flush_buffer(conn, buffer)

                    # Flush any remaining records for this timestamp
                    if buffer.tell() > 0:
                        self._flush_buffer(conn, buffer)

                    # Commit after each timestamp (every ~1M records for 1M meters)
                    conn.commit()

                    # Progress reporting
                    if ts_idx % 10 == 0 or ts_idx == total_timestamps:
                        elapsed = time.time() - start_backfill
                        rate = records_inserted / elapsed
                        progress = (ts_idx / total_timestamps) * 100
                        logger.info(f"  Progress: {progress:.1f}% | {records_inserted:,} records | {rate:.0f} rec/sec | Timestamp {ts_idx}/{total_timestamps}")

                backfill_duration = time.time() - start_backfill
                logger.info(f"\n✅ Backfill complete!")
                logger.info(f"  Records inserted:   {records_inserted:,}")
                logger.info(f"  Duration:           {backfill_duration:.1f}s")
                logger.info(f"  Throughput:         {records_inserted / backfill_duration:.0f} records/sec")

                # Get post-backfill stats (before compression)
                logger.info(f"\n📊 Database state after backfill (UNCOMPRESSED):")
                post_size, post_size_bytes = self.get_database_size(conn)
                post_stats = self.get_table_stats(conn)
                logger.info(f"  Database size:      {post_size}")
                logger.info(f"  Total records:      {post_stats['record_count']:,}")
                logger.info(f"  Table size:         {post_stats['total_size']}")
                logger.info(f"  Chunks:             {post_stats['total_chunks']} " +
                          f"({post_stats['compressed_chunks']} compressed, " +
                          f"{post_stats['uncompressed_chunks']} uncompressed)")
                logger.info(f"  Size increase:      {(post_size_bytes - initial_size_bytes) / (1024**3):.2f} GB")

                # Trigger compression for eligible chunks
                logger.info(f"\n🗜️  Triggering compression for chunks older than 1 day...")
                compression_start = time.time()

                with conn.cursor() as cur:
                    # Compress chunks older than 1 day (matches updated compression policy)
                    cur.execute("""
                        SELECT compress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass)
                        FROM timescaledb_information.chunks
                        WHERE hypertable_name = 'raw_meter_readings'
                          AND is_compressed = false
                          AND range_end < NOW() - INTERVAL '1 day'
                    """)
                    compressed_count = cur.rowcount
                    conn.commit()

                compression_duration = time.time() - compression_start
                logger.info(f"  Compressed chunks:  {compressed_count}")
                logger.info(f"  Duration:           {compression_duration:.1f}s")

                # Get final stats (after compression)
                logger.info(f"\n📊 Final database state (AFTER COMPRESSION):")
                final_size, final_size_bytes = self.get_database_size(conn)
                final_stats = self.get_table_stats(conn)
                logger.info(f"  Database size:      {final_size}")
                logger.info(f"  Total records:      {final_stats['record_count']:,}")
                logger.info(f"  Table size:         {final_stats['total_size']}")
                logger.info(f"  Chunks:             {final_stats['total_chunks']} " +
                          f"({final_stats['compressed_chunks']} compressed, " +
                          f"{final_stats['uncompressed_chunks']} uncompressed)")

                # Calculate compression ratio
                size_added = post_size_bytes - initial_size_bytes
                size_after_compression = final_size_bytes - initial_size_bytes

                if size_added > 0:
                    compression_ratio = size_added / size_after_compression
                    space_saved = size_added - size_after_compression
                    logger.info(f"\n💾 Compression Analysis:")
                    logger.info(f"  Uncompressed size:  {size_added / (1024**3):.2f} GB")
                    logger.info(f"  Compressed size:    {size_after_compression / (1024**3):.2f} GB")
                    logger.info(f"  Compression ratio:  {compression_ratio:.2f}x")
                    logger.info(f"  Space saved:        {space_saved / (1024**3):.2f} GB ({space_saved/size_added*100:.1f}%)")

                logger.info("=" * 80)

        except Exception as e:
            logger.error(f"❌ Backfill failed: {e}")
            raise

    def _flush_buffer(self, conn, buffer: io.StringIO):
        """Flush buffer to database using COPY"""
        buffer.seek(0)
        with conn.cursor() as cur:
            with cur.copy("""
                COPY raw_meter_readings (
                    reading_timestamp,
                    meter_id,
                    reading_consumption_milliwatts,
                    reading_production_milliwatts,
                    status,
                    arrived_at
                ) FROM STDIN
            """) as copy:
                copy.write(buffer.read())
        buffer.seek(0)
        buffer.truncate(0)


def main():
    parser = argparse.ArgumentParser(description='Backfill historical meter readings')
    parser.add_argument('--days', type=int, default=2, help='Number of days to backfill (default: 2)')
    parser.add_argument('--meters', type=int, default=1000000, help='Number of meters (default: 1,000,000)')
    parser.add_argument('--batch-size', type=int, default=100000, help='Batch size for inserts (default: 100,000)')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host (default: localhost)')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port (default: 5432)')
    parser.add_argument('--database', default='smart_meter_db', help='Database name (default: smart_meter_db)')
    parser.add_argument('--user', default='meter_admin', help='Database user (default: meter_admin)')
    parser.add_argument('--password', default='meter_pass_2024', help='Database password')

    args = parser.parse_args()

    # Build connection string
    conn_string = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"

    # Run backfill
    backfiller = HistoricalDataBackfiller(conn_string)
    backfiller.backfill(
        days=args.days,
        meter_count=args.meters,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()
