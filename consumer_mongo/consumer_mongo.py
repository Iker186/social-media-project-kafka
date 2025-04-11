from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import os

KAFKA_BROKER = os.getenv('KAFKA_SERVER')
TOPIC = os.getenv('KAFKA_TOPIC_MONGO', 'results_topic_mongo')
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = 'social_data'
COLLECTION_NAME = 'results'

try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print("✅ Conexión con MongoDB exitosa.")
except Exception as e:
    print(f"❌ Error al conectar con MongoDB: {e}")
    exit(1)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
    sasl_plain_username=os.getenv("KAFKA_USERNAME"),
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=20000  
)

message_count = 0
skip_count = 0

for message in consumer:
    record = message.value

    if record.get("source") != "mongo":
        continue
    
    user_id = record.get("user_id")

    if collection.find_one({"user_id": user_id}):
        print(f"[⏭] Registro con user_id {user_id} ya existe. Saltando.")
        skip_count += 1
        continue

    try:
        collection.insert_one(record)
        print(f"[✓] Insertado en MongoDB: {user_id}")
        message_count += 1
    except Exception as e:
        print(f"[❌] Error al insertar: {e}")

consumer.close()
print(f"\n📦 Proceso finalizado: {message_count} insertados, {skip_count} duplicados.\n")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/get-mongo-data")
def get_data_mongo():
    try:
        data = list(collection.find({}, {"_id": 0})) 
        return {"status": "ok", "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
