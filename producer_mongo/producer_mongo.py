from flask import Flask, jsonify
from confluent_kafka import Producer
import requests
import logging
import csv
import io
import json
import os

# Configuración de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

# Configuración del productor Kafka
PRODUCER_CONF = {
    'bootstrap.servers': os.getenv('KAFKA_SERVER'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM', 'SCRAM-SHA-256'),
    'sasl.username': os.getenv('KAFKA_USERNAME'),
    'sasl.password': os.getenv('KAFKA_PASSWORD'),
}

producer = Producer(PRODUCER_CONF)

TOPIC = os.getenv("KAFKA_TOPIC_MONGO", "results_topic_mongo")
DATA_URL = os.getenv("MONGO_DATA_URL", "https://raw.githubusercontent.com/Iker186/streamlit-social-media/refs/heads/main/results/processed_data.json/part-00000-b71226d5-3187-479e-888f-23897cd4299a-c000.json")

def delivery_report(err, msg):
    if err:
        logging.error(f"Error al enviar mensaje: {err}")
    else:
        logging.info(f"Mensaje enviado a {msg.topic()}: {msg.value().decode('utf-8')}")

@app.route("/send-to-kafka-mongo", methods=["POST"])
def send_data_mongo():
    try:
        logging.info(f"Descargando datos desde: {DATA_URL}")
        response = requests.get(DATA_URL)
        response.raise_for_status()

        decoded_content = response.content.decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(decoded_content))

        count = 0
        for row in csv_reader:
            record = {
                "user_id": row.get('UserID'),
                "name": row.get('Name'),
                "gender": row.get('Gender'),
                "dob": row.get('DOB'),
                "interests": row.get('Interests'),
                "city": row.get('City'),
                "country": row.get('Country'),
                "source": "mongo"
            }

            message = json.dumps(record)
            producer.produce(TOPIC, message.encode("utf-8"), callback=delivery_report)
            count += 1

        producer.flush()
        logging.info(f"Total de registros enviados: {count}")
        return jsonify({"status": "success", "message": f"{count} registros enviados al tópico '{TOPIC}'"}), 200

    except Exception as e:
        logging.error("Error al enviar los datos", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/health")
def health():
    return "ok", 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))  
    app.run(host="0.0.0.0", port=port)
