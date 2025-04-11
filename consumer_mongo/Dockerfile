FROM python:3.11-slim

WORKDIR /

COPY . .

RUN pip install --no-cache-dir kafka-python psycopg2-binary uvicorn

EXPOSE 8000

CMD ["sh", "-c", "python consumer_mongo.py & uvicorn api:app --host 0.0.0.0 --port 8000"]
