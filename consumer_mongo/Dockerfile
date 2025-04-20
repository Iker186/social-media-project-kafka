FROM python:3.11-slim

WORKDIR /app

COPY consumer_mongo/consumer_mongo.py .

RUN apt-get update && \
    apt-get install -y gcc librdkafka-dev curl && \
    pip install --no-cache-dir psycopg2-binary fastapi uvicorn confluent-kafka && \
    apt-get remove -y gcc && apt-get autoremove -y && apt-get clean

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s \
  CMD curl --fail http://localhost:8000/get-data-mongo || exit 1

CMD ["uvicorn", "consumer_mongo:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
