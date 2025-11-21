# Smart Meter Data Pipeline

A scalable data pipeline for ingesting and analyzing electricity smart meter readings from 1 million meters with 15-minute granularity.

## Architecture

- **TimescaleDB**: Time-series database for storing meter readings
- **Apache Kafka**: Message broker for streaming meter data
- **Python Simulator**: Generates realistic meter readings
- **Kafka Consumer**: Ingests data from Kafka to TimescaleDB
- **DBT**: Data transformation and analytics layer

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- At least 8GB RAM available for Docker
- At least 10GB of Disk space if you want to run the simulation for a few hours

### 1. Spin up architecture

```bash
# Navigate to the root of the cloned repository
cd smart_meter_data_pipeline

# Make the database init scripts readable on your system
chmod 644 database/init_scripts/01_create_schema.sql
chmod 644 database/init_scripts/02_populate_dimensions.sql

# Build and run the Docker compose in the background
docker compose up -d --build

# If you want to scale the consumer horizontally you can use the following argument (4 consumers is max right now efficiency wise)
docker compose up -d --build  --scale consumer=4

# On first startup this might take a few minutes depending on your systems capabilities
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
