FROM python:3.11-slim

WORKDIR /app

COPY consumer_postgres/consumer_postgres.py .

RUN apt-get update && \
    apt-get install -y gcc librdkafka-dev curl && \
    pip install --no-cache-dir psycopg2-binary fastapi uvicorn confluent-kafka && \
    apt-get remove -y gcc && apt-get autoremove -y && apt-get clean

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=5s \
  CMD curl --fail http://localhost:8001/get-data-postgres || exit 1

CMD ["uvicorn", "consumer_postgres:app", "--host", "0.0.0.0", "--port", "8001", "--log-level", "info"]
