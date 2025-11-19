FROM python:3.13-slim

WORKDIR /app

# Install build dependencies for confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY kafka_monitor.py .

# Make executable
RUN chmod +x kafka_monitor.py

CMD ["python", "kafka_monitor.py"]
