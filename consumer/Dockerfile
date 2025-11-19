# Smart Meter Data Consumer Dockerfile
FROM python:3.13-slim

# Install system dependencies for confluent-kafka (requires librdkafka)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        librdkafka-dev \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first (for better Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY meter_consumer.py .

# Create non-root user for security
RUN useradd -m -u 1000 consumer && \
    chown -R consumer:consumer /app

USER consumer

# Run the consumer
CMD ["python", "-u", "meter_consumer.py"]
