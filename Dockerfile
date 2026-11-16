FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    SAMPLE_MINIMIZER_WORKDIR=/data/jobs

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Application code and demo data.
COPY app /app/app
COPY demo_data /app/demo_data

# Trial artifacts (original uploads + minimal samples) live on a volume.
RUN mkdir -p /data/jobs
VOLUME ["/data"]

EXPOSE 8000

# A single uvicorn worker is intentional: job state is in-process. Trial
# isolation is provided by subprocesses (one fresh DuckDB per trial), not by
# multiple server workers.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
