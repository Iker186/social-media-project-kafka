FROM python:3.11-slim

WORKDIR /app

COPY consumer_postgres/consumer_postgres.py .

RUN apt-get update && apt-get install -y gcc librdkafka-dev && \
    pip install --no-cache-dir psycopg2-binary fastapi uvicorn confluent-kafka && \
    apt-get remove -y gcc && apt-get autoremove -y && apt-get clean

EXPOSE 8001

CMD ["python", "consumer_postgres.py"]
