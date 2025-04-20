FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev libffi-dev librdkafka-dev curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && \
    apt-get remove -y gcc python3-dev libffi-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY producer_postgres.py .

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["python", "producer_postgres.py"]
