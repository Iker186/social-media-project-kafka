from fastapi import FastAPI
import pandas as pd
from kafka import KafkaProducer
import json
import os

app = FastAPI()

KAFKA_BROKER = os.getenv('KAFKA_SERVER')
TOPIC = os.getenv('KAFKA_TOPIC_POSTGRES', 'results_topic_pg')

@app.post("/send-to-kafka")
def send_to_kafka_postgres():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
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
                "country": row['Country']
            }
            producer.send(TOPIC, value=record)
            print(f"[→] Enviado a Kafka (Postgres): {record}")

        producer.flush()
        producer.close()
        return {"status": "OK", "message": "Datos enviados a Kafka (Postgres)"}
    
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}
