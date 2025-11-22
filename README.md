# Smart Meter Data Pipeline

A scalable data pipeline for ingesting and analyzing electricity smart meter readings from 1 million meters with 15-minute granularity.


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

# On first startup this might take a few minutes depending on your systems capabilities for build and database init and populating the dimensions
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

## Architecture

- **TimescaleDB**: Time-series database for storing meter readings
- **Apache Kafka**: Message broker for streaming meter data
- **Python Simulator**: Generates realistic meter readings
- **Kafka Consumer**: Ingests data from Kafka to TimescaleDB
- **DBT**: Data transformation and analytics layer

## Design Decisions

### Why TimescaleDB?
- **Time-series optimization**: Built for high-volume time-series data (96M+ rows/day)
- **Automatic compression**: Achieves 2-4x compression ratio
- **PostgreSQL compatibility**: Works seamlessly with dbt and SQL tooling
- **Hypertable partitioning**: Automatic 1-day chunks optimize query performance

### Why Apache Kafka?
- **Handles burst traffic**: 1M meters sending readings simultaneously
- **Decouples producer from consumer**: Producer can send regardless of consumer state
- **At-least-once delivery**: Ensures no data loss with idempotent consumer handling (ON CONFLICT)
- **Horizontal scalability**: 4 partitions allow up to 4 parallel consumers

### Key Performance Optimizations
- **Idempotent writes**: `ON CONFLICT DO NOTHING` prevents duplicates from Kafka redelivery
- **Storage optimization**: INTEGER (4 bytes) vs BIGINT (8 bytes), CHAR(1) for status codes
- **Partitioning strategy**: Kafka partitions by meter_id to maintain ordering for LAG() calculations

## Scalability Considerations

The architecture is designed to scale from 1M to 5M+ meters:

### Horizontal Scaling Path
- **Kafka partitions**: Currently 4 partitions → scale to 16+ for higher parallelism
- **Consumer instances**: Scale from 1 to 16+ consumers (one per partition)
- **Database sharding**: TimescaleDB supports distributed hypertables for multi-node clusters

### Scaling to 5M Meters (5x Growth)

**Current capacity:** 1M meters × 96 readings/day = 96M rows/day
**Target capacity:** 5M meters × 96 readings/day = 480M rows/day

**Solution:**
- Scale Kafka partitions: 4 → 16
- Scale consumers: 4 → 16 (at 10K rec/sec each = 160K rec/sec)
- Result: 13.8B rows/day capacity (28x headroom for future growth)

## Assumptions

### Data Model Simplifications
- **Residential meters only**: All 1M meters are residential with 1:1 customer:meter relationship
- **Single tariff rate**: $0.28/kWh flat rate for all customers (could extend to time-of-use pricing)
- **50% solar adoption**: Represents progressive market with high renewable penetration

### Operational Assumptions
- **No historical data**: Meters start at reading 0 on producer startup (not persisted between restarts)
- **Data quality**: 98% valid readings, 1.5% estimated, 0.5% error
- **No late-arriving data handling**: Assumes real-time ingestion with no backfill (production would need late data policies, interpolation for gaps, and extrapolation for estimates)

### dbt Modeling Decisions
- **Staging as VIEW**: Recalculates LAG() on every query (production would materialize as incremental table or use TimescaleDB continous aggregates)
- **Full refresh marts**: Daily billing and grid load tables rebuild completely
- **No data quality tests**: Would add dbt tests for null checks, referential integrity, and outlier detection

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
