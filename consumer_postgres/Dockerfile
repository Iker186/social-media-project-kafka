FROM python:3.11-slim

WORKDIR /app

# Copia el archivo desde el subdirectorio
COPY consumer_postgres/consumer_postgres.py /app/consumer_postgres.py

RUN apt-get update && apt-get install -y gcc librdkafka-dev && \
    pip install --no-cache-dir psycopg2-binary fastapi uvicorn confluent-kafka && \
    apt-get remove -y gcc && apt-get autoremove -y && apt-get clean

EXPOSE 8001

CMD ["uvicorn", "consumer_postgres:app", "--host", "0.0.0.0", "--port", "8001"]
