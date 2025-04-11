from fastapi import FastAPI
import pandas as pd
from kafka import KafkaProducer
import json
import os

app = FastAPI()

KAFKA_BROKER = os.getenv('KAFKA_SERVER')
TOPIC = os.getenv('KAFKA_TOPIC_MONGO', 'results_topic_mongo')

@app.get("/")
def health_check_mongo():
    return {"status": "ok", "message": "Producer Mongo running"}

@app.post("/send-to-kafka-mongo")
def send_to_kafka():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
            sasl_plain_username=os.getenv("KAFKA_USERNAME"),
            sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        data = pd.read_csv('./data/social_media.csv')

        for _, row in data.iterrows():
            record = {
                "user_id": row['UserID'],
                "name": row['Name'],
                "gender": row['Gender'],
                "dob": row['DOB'],
                "interests": row['Interests'],
                "city": row['City'],
                "country": row['Country'],
                "source": "mongo"
            }
            producer.send(TOPIC, value=record)
            print(f"[→] Enviado a Kafka (Mongo): {record}")

        producer.flush()
        producer.close()
        return {"status": "OK", "message": "Datos enviados a Kafka"}
    
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}
