from kafka import KafkaConsumer
import json
import psycopg2
import os

# Configuración de Kafka y PostgreSQL
KAFKA_BROKER = os.getenv('KAFKA_SERVER')
TOPIC = os.getenv('KAFKA_TOPIC_POSTGRES', 'results_topic')
MAX_MENSAJES = 100000

POSTGRES_CONFIG = {
    "dbname": os.getenv('POSTGRES_DB'),
    "user": os.getenv('POSTGRES_USER'),
    "password": os.getenv('POSTGRES_PASSWORD'),
    "host": os.getenv('POSTGRES_HOST'),
    "port": os.getenv('POSTGRES_PORT', '5432')
}

try:
    print(f"📡 POSTGRES_HOST recibido: '{POSTGRES_CONFIG['host']}'")
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    # Crear la tabla si no existe
    cur.execute("""
    CREATE TABLE IF NOT EXISTS results (
        user_id INT PRIMARY KEY,
        name VARCHAR(255),
        gender VARCHAR(10),
        dob DATE,
        interests TEXT,
        city VARCHAR(255),
        country VARCHAR(255)
    )
    """)
    conn.commit()
    print("✅ Tabla creada o verificada en PostgreSQL.")

    # Verificar si ya existen los 100,000 registros
    cur.execute("SELECT COUNT(*) FROM results")
    total_existentes = cur.fetchone()[0]

    if total_existentes >= MAX_MENSAJES:
        print(f"⏭ Ya existen {total_existentes} registros. Saltando proceso.")
        cur.close()
        conn.close()
        exit(0)

except Exception as e:
    print(f"❌ Error al conectar a PostgreSQL: {e}")
    exit(1)

# Consumidor de Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
    sasl_plain_username=os.getenv("KAFKA_USERNAME"),
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=20000  # Opcional para cortar si no llegan más mensajes
)

print(f"🛰 Escuchando mensajes del topic '{TOPIC}'...")

insertados = 0
omitidos = 0

for message in consumer:
    if insertados >= MAX_MENSAJES:
        print(f"✅ Procesados {insertados} mensajes. Finalizando consumidor.")
        break

    record = message.value
    user_id = record.get('user_id')

    # Comprobar si ya existe ese user_id
    cur.execute("SELECT 1 FROM results WHERE user_id = %s", (user_id,))
    if cur.fetchone():
        print(f"[⏭] Registro con user_id {user_id} ya existe. Saltando.")
        omitidos += 1
        continue

    try:
        cur.execute("""
            INSERT INTO results (user_id, name, gender, dob, interests, city, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            user_id,
            record.get('name', 'N/A'),
            record.get('gender', 'N/A'),
            record.get('dob', '1900-01-01'),
            record.get('interests', 'N/A'),
            record.get('city', 'N/A'),
            record.get('country', 'N/A')
        ))
        conn.commit()
        insertados += 1
        print(f"[✓ {insertados}] Insertado en PostgreSQL: {user_id}")
    except Exception as e:
        print(f"❌ Error al insertar en PostgreSQL: {e}")
        conn.rollback()

consumer.close()
cur.close()
conn.close()

print(f"\n📦 Proceso finalizado: {insertados} insertados, {omitidos} duplicados.\n")
