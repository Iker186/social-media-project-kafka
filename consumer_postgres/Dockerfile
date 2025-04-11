FROM python:3.11-slim

WORKDIR /app

COPY consumer_postgres/consumer_postgres.py ./

RUN pip install --no-cache-dir kafka-python psycopg2-binary

CMD ["python", "consumer_postgres.py"]
