import threading
import logging
import json
import time
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# Configuración de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# FastAPI setup
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Kafka
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_SERVER'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM', 'SCRAM-SHA-256'),
    'sasl.username': os.getenv('KAFKA_USERNAME'),
    'sasl.password': os.getenv('KAFKA_PASSWORD'),
    'group.id': 'mongo-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}
TOPIC = os.getenv('KAFKA_TOPIC_MONGO', 'results_topic_mongo')

# MongoDB
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = 'social_data'
COLLECTION_NAME = 'results'

try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logging.info("✅ Conexión con MongoDB exitosa.")
except Exception as e:
    logging.error(f"❌ Error al conectar con MongoDB: {e}", exc_info=True)
    raise SystemExit(1)

def kafka_consumer_loop():
    while True:
        try:
            consumer = Consumer(KAFKA_CONFIG)
            consumer.subscribe([TOPIC])
            logging.info(f"🛰 Escuchando tópico '{TOPIC}'...")

            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logging.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    record = json.loads(msg.value().decode('utf-8'))
                    if record.get("source") != "mongo":
                        continue

                    # Soporte para user_id o UserID
                    user_id = record.get("user_id") or record.get("UserID")

                    if not isinstance(user_id, (str, int)) or str(user_id).strip() == "":
                        logging.warning(f"⚠️ user_id inválido o ausente en el mensaje: {record}")
                        continue

                    if collection.find_one({"UserID": user_id}):
                        logging.info(f"[⏭] UserID {user_id} ya existe. Saltando.")
                        continue

                    collection.insert_one(record)
                    logging.info(f"[✓] Insertado en MongoDB: {user_id}")
                    consumer.commit()

                except json.JSONDecodeError as e:
                    logging.warning(f"⚠️ Error decodificando JSON: {e}")
                except Exception as e:
                    logging.error(f"❌ Error al insertar en MongoDB: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"❌ Error crítico en Kafka loop. Reintentando en 5s...", exc_info=True)
            time.sleep(5)
        finally:
            try:
                consumer.close()
                logging.info("📴 Kafka consumer cerrado.")
            except:
                pass

def heartbeat():
    while True:
        logging.info("✅ Servicio activo (Mongo Consumer)...")
        time.sleep(60)

@app.on_event("startup")
def start_background_tasks():
    threading.Thread(target=kafka_consumer_loop).start()
    threading.Thread(target=heartbeat).start()

@app.get("/get-data-mongo")
def get_data_mongo():
    try:
        data = list(collection.find({}, {"_id": 0}))  # Excluye el _id de Mongo
        return {"status": "ok", "data": data}
    except Exception as e:
        logging.error("❌ Error al obtener datos de MongoDB:", exc_info=True)
        return {"status": "error", "message": str(e)}
