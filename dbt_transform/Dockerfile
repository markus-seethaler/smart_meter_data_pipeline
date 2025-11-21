FROM python:3.13-slim

WORKDIR /dbt

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dbt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy dbt project files
COPY . .

# Set environment variable for dbt profiles
ENV DBT_PROFILES_DIR=/dbt

# Default command - can be overridden in docker-compose
CMD ["dbt", "debug"]
