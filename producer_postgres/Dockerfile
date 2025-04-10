FROM python:3.11-slim

WORKDIR /app

COPY producer_postgres.py ./
COPY data/ ./data/

RUN pip install --no-cache-dir fastapi pandas kafka-python uvicorn

EXPOSE 8000

CMD ["uvicorn", "producer_postgres:app", "--host", "0.0.0.0", "--port", "8000"]
