FROM python:3.11-slim

WORKDIR /app

COPY consumer_mongo/consumer_mongo.py .

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl librdkafka-dev && \
    pip install --no-cache-dir kafka-python pymongo fastapi uvicorn && \
    apt-get remove -y curl && apt-get autoremove -y && apt-get clean

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s \
  CMD curl --fail http://localhost:8000/ || exit 1

CMD ["uvicorn", "consumer_mongo:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
