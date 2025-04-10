# Usa una imagen ligera con Python y Uvicorn
FROM python:3.11-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos necesarios
COPY producer_mongo.py ./
COPY data/ ./data/

# Instala dependencias
RUN pip install --no-cache-dir fastapi pandas kafka-python uvicorn

# Expone el puerto que usará FastAPI
EXPOSE 8000

# Comando para iniciar la API
CMD ["uvicorn", "producer_mongo:app", "--host", "0.0.0.0", "--port", "8000"]
