FROM python:3.9-slim

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY lineage/ ./lineage/
COPY examples/ ./examples/

# Expose the FastAPI port
EXPOSE 8000

# Default command: launch the API server
CMD ["uvicorn", "lineage.api:app", "--host", "0.0.0.0", "--port", "8000"]
