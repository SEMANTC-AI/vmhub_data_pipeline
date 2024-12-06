FROM python:3.9-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p credentials schemas

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and schemas
COPY src/ ./src/
COPY schemas/ ./schemas/

CMD ["python", "-m", "src.main"]