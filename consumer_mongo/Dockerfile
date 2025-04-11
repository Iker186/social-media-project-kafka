FROM python:3.11-slim

WORKDIR /app

COPY consumer_mongo/consumer_mongo.py ./

RUN pip install --no-cache-dir kafka-python pymongo

CMD ["python", "consumer_mongo.py"]
