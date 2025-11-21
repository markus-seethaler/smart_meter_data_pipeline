# Smart Meter Data Pipeline

A scalable data pipeline for ingesting and analyzing electricity smart meter readings from 1 million meters with 15-minute granularity.

## Architecture

- **TimescaleDB**: Time-series database for storing meter readings
- **Apache Kafka**: Message broker for streaming meter data
- **Python Simulator**: Generates realistic meter readings
- **Kafka Consumer**: Ingests data from Kafka to TimescaleDB
- **DBT**: Data transformation and analytics layer

## Design Decisions

### Why TimescaleDB?
- **Time-series optimization**: Built for high-volume time-series data (96M+ rows/day)
- **Automatic compression**: Achieves 2-4x compression ratio, reducing storage costs by ~75%
- **PostgreSQL compatibility**: Works seamlessly with dbt and SQL tooling
- **Hypertable partitioning**: Automatic 1-day chunks optimize query performance
- **Production-ready**: Scales to billions of rows with minimal configuration

### Why Cumulative Meter Readings?
- **Realistic simulation**: Matches real smart meter behavior
- **Accurate energy accounting**: Using LAG() window functions to calculate period deltas (loadprofiles) from cumulative values

### Why Apache Kafka?
- **Handles burst traffic**: 1M meters sending readings simultaneously
- **Decouples producer from consumer**: Producer can send regardless of consumer state
- **At-least-once delivery**: Ensures no data loss with idempotent consumer handling (ON CONFLICT)
- **Horizontal scalability**: 4 partitions allow up to 4 parallel consumers
- **Message retention**: 7-day retention provides replay capability

### Key Performance Optimizations
- **Batch inserts with executemany()**: 3-10K records/sec per consumer (3-5x faster than row-by-row)
- **Idempotent writes**: `ON CONFLICT DO NOTHING` prevents duplicates from Kafka redelivery
- **Storage optimization**: INTEGER (4 bytes) vs BIGINT (8 bytes), CHAR(1) for status codes
- **Partitioning strategy**: Kafka partitions by meter_id to maintain ordering for LAG() calculations

## Scalability Considerations

The architecture is designed to scale from 1M to 5M+ meters (per case study requirements):

### Horizontal Scaling Path
- **Kafka partitions**: Currently 4 partitions → scale to 16+ for higher parallelism
- **Consumer instances**: Scale from 1 to 16+ consumers (one per partition)
- **Database sharding**: TimescaleDB supports distributed hypertables for multi-node clusters

### Current Performance
- **Producer throughput**: ~500K-1M messages/sec (negligible bottleneck)
- **Consumer throughput**: 3-10K records/sec per consumer
- **Database capacity**: TimescaleDB handles billions of rows with compression

### Scaling to 5M Meters
```
Current:  1M meters × 96 readings/day = 96M rows/day
Target:   5M meters × 96 readings/day = 480M rows/day

Solution:
- 16 Kafka partitions
- 16 consumer instances × 10K rec/sec = 160K rec/sec capacity
- 160K rec/sec × 86,400 sec/day = 13.8B rows/day (28x headroom)
```

### Bottleneck Management
1. **Database writes**: Mitigated by batch inserts and TimescaleDB compression
2. **Network bandwidth**: Mitigated by Kafka snappy compression (~2-3x reduction)
3. **Disk I/O**: Mitigated by hypertable chunking and SSD storage
4. **Query performance**: Mitigated by proper indexing and compression policies

### Cost Optimization
- **Compression**: Reduces storage by 75% after 1 day (automatic)
- **Storage optimization**: INTEGER vs BIGINT saves ~50% per record

## Assumptions

### Data Model Simplifications
- **Residential meters only**: All 1M meters are residential with 1:1 customer:meter relationship
- **Single tariff rate**: $0.28/kWh flat rate for all customers (could extend to time-of-use pricing)
- **15-minute intervals**: Standard smart meter reading frequency per industry practice
- **50% solar adoption**: Represents progressive market with high renewable penetration

### Operational Assumptions
- **At-least-once delivery**: Kafka consumer may receive duplicate messages (handled by ON CONFLICT)
- **No historical data**: Meters start at reading 0 on producer startup (not persisted between restarts)
- **Data quality**: 98% valid readings, 1.5% estimated, 0.5% error (realistic utility distribution)
- **No late-arriving data handling/Interpolation**: Assumes real-time ingestion (production would need late data policies/Interpolation/Extrapolation)

### Infrastructure Simplifications
- **Single Kafka broker**: Demo environment (production requires 3+ brokers for high availability)
- **No authentication**: Open connections for demo purposes
- **No TLS encryption**: Unencrypted traffic (would add SSL/TLS in production)
- **No monitoring/alerting**: Basic logging only (would add Prometheus/Grafana in production)

### dbt Modeling Decisions
- **Staging as VIEW**: Recalculates LAG() on every query (production would materialize as incremental table)
- **Full refresh marts**: Daily billing and grid load tables rebuild completely (acceptable for demo scope)
- **No data quality tests**: Would add dbt tests for null checks, referential integrity, and outlier detection

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- At least 8GB RAM available for Docker
- At least 10GB of Disk space if you want to run the simulation for a few hours

### 1. Spin up architecture

Developed on Linux adapt to your systems requirements.

```bash
# Navigate to the root of the cloned repository
cd smart_meter_data_pipeline

# Make the database init scripts readable on your system
chmod 644 database/init_scripts/01_create_schema.sql
chmod 644 database/init_scripts/02_populate_dimensions.sql

# Build and run the Docker compose in the background
docker compose up -d --build

# If you want to scale the consumer horizontally you can use the following argument (4 consumers is max right now efficiency wise because of 4 partitions)
docker compose up -d --build  --scale consumer=4

# If you change variables either in code or the .env sometimes Docker caching does not transfer the changes. If this is the case run
docker compose build --no-cache && docker compose up -d --scale consumer=4

# On first startup this might take a few minutes depending on your systems capabilities for build and database init and population
```

### 2. Watch the ingestion process via the logs

```bash
# Watch all logs
docker compose logs -f

# Watch a specific component
docker compose logs -f consumer
docker compose logs -f producer monitor
```

### 3. Run Analytics Transformations

After ~30 minutes of data collection, run dbt to generate analytics:

```bash
# Run dbt models (staging + marts)
docker compose run --rm dbt dbt run

# Query results
docker exec smart_meter_timescaledb psql -U meter_admin -d smart_meter_db \
  -c "SELECT * FROM fact_customer_billing_daily LIMIT 10;"
```

**Models created:**
- `stg_meter_readings` - Staging view with delta calculations
- `fact_customer_billing_daily` - Daily billing by customer
- `fact_grid_load_hourly` - Hourly grid load by zone

### 4. Stop Services

Keep in mind that the current meter reading of a meter is not persisted in this demo so if you restart all meters start with a 0 reading again.

```bash
docker-compose down

# To remove volumes and start fresh (delete all data):
docker-compose down -v
```

## Database Schema

### Dimension Tables

- **dim_meters**: Meter metadata (1M residential meters, 50% with solar)
  - meter_id (PK), meter_idn, customer_id (1:1 relationship)
  - malo_cons, malo_prod (solar production capability)
  - Gateway identifier, grid zone assignment

- **dim_customers**: Customer information (1M residential customers)
  - customer_id (PK), customer_name, account_status

- **dim_tariff_rates**: Single standard residential rate ($0.28/kWh)

### Fact Tables

- **raw_meter_readings**: Time-series cumulative readings (hypertable)
  - reading_timestamp, meter_id (composite PK)
  - reading_consumption_milliwatts, reading_production_milliwatts (cumulative)
  - status (V=Valid, E=Estimated, R=eRror)
  - arrived_at (ingestion timestamp)

### Performance Features

- **Hypertable**: 1-day chunks for optimal query performance
- **Compression**: Automatic compression after 1 day (3-4x ratio)
- **Indexes**: Optimized for meter_id + timestamp queries
- **Storage optimization**: INTEGER (4B) vs BIGINT, CHAR(1) for status

## Configuration

Environment variables are in `.env`:

- `METER_COUNT`: Number of meters to simulate (default: 1,000,000)
- `READING_INTERVAL_MINUTES`: Reading frequency (default: 15)
- `CONTINUOUS_FLOW`: Message sending mode (false=batch, true=continuous)
- `POSTGRES_DB`: Database name
- `POSTGRES_USER`: Database user
- `POSTGRES_PASSWORD`: Database password
- `POSTGRES_PORT`: Database port (default: 5432)

## Data Volume

- 1M meters × 96 readings/day = ~96M rows/day

## Troubleshooting

### Database initialization fails with "Permission denied"

If you see an error like `psql: error: /docker-entrypoint-initdb.d/01_create_schema.sql: Permission denied`:

```bash
# Fix file permissions
chmod 644 database/init_scripts/01_create_schema.sql

# Restart the database
docker compose down -v
docker compose up -d timescaledb
```

### Schema not created on restart

Database initialization scripts only run when the database is created for the first time. To recreate:

```bash
# Remove volumes and restart
docker compose down -v
docker compose up -d timescaledb
```
