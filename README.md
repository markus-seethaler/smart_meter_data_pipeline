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

### 3. Stop Services

Keep in mind that the current meter reading of a meter is not persisted in this demo so if you restart all meters start with a 0 reading again.

```bash
docker-compose down

# To remove volumes and start fresh (delete all data):
docker-compose down -v
```

## Database Schema

### Dimension Tables

- **dim_meters**: Meter metadata (1M meters)
  - meter_id (PK), meter_idn, customer_id
  - Location: street, house_number, zip, city
  - Transformation factors for current and voltage
  - Gateway identifier

- **dim_customers**: Customer information
  - customer_id (PK), customer_name, customer_type, account_status

### Fact Tables

- **raw_meter_readings**: Time-series readings (hypertable)
  - reading_timestamp, meter_id
  - reading_mw (milliwatts for precision without decimals)
  - status (valid/estimated/error)
  - arrived_at (ingestion timestamp)

### Performance Features

- **Hypertable**: 1-day chunks for optimal query performance
- **Compression**: Automatic compression after 1 day
- **Indexes**: Optimized for meter_id + timestamp queries

## Configuration

Environment variables are in `.env`:

- `POSTGRES_DB`: Database name
- `POSTGRES_USER`: Database user
- `POSTGRES_PASSWORD`: Database password
- `POSTGRES_PORT`: Database port (default 5432)

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
