FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir kafka-python psycopg2-binary

EXPOSE 8000

CMD ["sh", "-c", "python consumer.py & uvicorn api:app --host 0.0.0.0 --port 8000"]
