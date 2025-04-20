import threading
import logging
import json
import os
import time
from datetime import datetime, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Consumer, KafkaError
import psycopg2

KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_SERVER'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM', 'PLAIN'),
    'sasl.username': os.getenv('KAFKA_USERNAME'),
    'sasl.password': os.getenv('KAFKA_PASSWORD'),
    'group.id': 'postgres-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

POSTGRES_CONFIG = {
    "dbname": os.getenv('POSTGRES_DB'),
    "user": os.getenv('POSTGRES_USER'),
    "password": os.getenv('POSTGRES_PASSWORD'),
    "host": os.getenv('POSTGRES_HOST'),
    "port": os.getenv('POSTGRES_PORT', '5432')
}

TOPIC = os.getenv('KAFKA_TOPIC_POSTGRES', 'results_postgres')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)

def create_table():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS results (
                        user_id INT PRIMARY KEY,
                        gender VARCHAR(10),
                        dob DATE,
                        interests TEXT,
                        city VARCHAR(255),
                        country VARCHAR(255)
                    )
                """)
                conn.commit()
        logging.info("✅ Tabla 'results' verificada/creada.")
    except Exception as e:
        logging.error(f"❌ Error creando/verificando tabla: {e}", exc_info=True)

create_table()

def transformar_datos(raw):
    try:
        user_id = int(raw.get("UserID", 0))
        if user_id == 0:
            raise ValueError
    except:
        user_id = None

    try:
        edad = int(raw.get("Edad", 0))
        dob = datetime.today() - timedelta(days=edad * 365)
        dob_str = dob.strftime("%Y-%m-%d")
    except:
        dob_str = "1900-01-01"

    return {
        "user_id": user_id,
        "gender": raw.get("Gender", "N/A"),
        "dob": dob_str,
        "interests": raw.get("Interests", "N/A"),
        "city": raw.get("City", "N/A"),
        "country": raw.get("Country", "N/A")
    }

def insert_record(data: dict):
    user_id = data.get("user_id")
    if not user_id:
        logging.error(f"❌ user_id inválido en los datos: {data}")
        return False

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO results (user_id, gender, dob, interests, city, country)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO NOTHING;
                """, (
                    user_id,
                    data.get("gender", 'N/A'),
                    data.get("dob", '1900-01-01'),
                    data.get("interests", 'N/A'),
                    data.get("city", 'N/A'),
                    data.get("country", 'N/A')
                ))
                conn.commit()
                logging.info(f"[✓] Insertado en BD: {user_id}")
                return True
    except Exception as e:
        logging.error(f"❌ Error al insertar en PostgreSQL: {e}", exc_info=True)
        return False

def kafka_consumer_loop():
    while True:
        try:
            consumer = Consumer(KAFKA_CONFIG)
            consumer.subscribe([TOPIC])
            logging.info(f"🛰 Conectado y escuchando tópico '{TOPIC}'...")

            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logging.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    raw = json.loads(msg.value().decode('utf-8'))
                    data = transformar_datos(raw)

                    if insert_record(data):
                        consumer.commit(asynchronous=False)
                except json.JSONDecodeError as e:
                    logging.warning(f"⚠️ Error de decodificación JSON: {e}")
        except Exception as e:
            logging.error(f"❌ Error fatal en Kafka loop, reintentando en 5s: {e}", exc_info=True)
            time.sleep(5)
        finally:
            try:
                consumer.close()
                logging.info("📴 Kafka consumer cerrado.")
            except:
                pass

def heartbeat():
    while True:
        logging.info("✅ Servicio activo...")
        time.sleep(60)

@app.on_event("startup")
def start_background_tasks():
    threading.Thread(target=kafka_consumer_loop).start()
    threading.Thread(target=heartbeat).start()

@app.get("/get-data-postgres")
def get_data_postgres():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM results")
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                data = [dict(zip(columns, row)) for row in rows]
                return {"status": "ok", "data": data}
    except Exception as e:
        logging.error("❌ Error al obtener datos:", exc_info=True)
        return {"status": "error", "message": str(e)}
