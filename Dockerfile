# Use official Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python scripts
COPY . .

# Default command to run the producer script
CMD ["python", "kraken_to_kafka.py"]
