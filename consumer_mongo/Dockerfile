FROM python:3.11-slim

WORKDIR /app

COPY consumer_mongo/consumer_mongo.py .
COPY consumer_mongo/api.py .

RUN pip install --no-cache-dir kafka-python pymongo fastapi uvicorn

EXPOSE 8000

CMD ["sh", "-c", "python consumer_mongo.py & uvicorn api:app --host 0.0.0.0 --port 8000"]
