# Usa una imagen ligera con Python
FROM python:3.11-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos necesarios
COPY consumer_mongo.py ./

# Instala dependencias
RUN pip install --no-cache-dir kafka-python pymongo

# Ejecuta el script de consumidor
CMD ["python", "consumer_mongo.py"]
