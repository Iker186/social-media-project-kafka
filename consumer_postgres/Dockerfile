FROM python:3.11-slim

WORKDIR /app

COPY consumer_postgres/consumer_postgres.py .
COPY consumer_postgres/api.py .

RUN pip install --no-cache-dir kafka-python psycopg2-binary fastapi uvicorn

EXPOSE 8001

CMD ["sh", "-c", "python consumer_postgres.py & uvicorn api:app --host 0.0.0.0 --port 8001"]
