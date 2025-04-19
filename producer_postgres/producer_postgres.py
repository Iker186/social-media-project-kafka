from flask import Flask, jsonify
from confluent_kafka import Producer
import requests
import logging
import os

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

# Leer configuración desde variables de entorno (seguridad)
PRODUCER_CONF = {
    'bootstrap.servers': os.getenv('KAFKA_SERVER'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM', 'PLAIN'),
    'sasl.username': os.getenv('KAFKA_USERNAME'),
    'sasl.password': os.getenv('KAFKA_PASSWORD'),
}

producer = Producer(PRODUCER_CONF)

TOPIC = os.getenv("KAFKA_TOPIC_POSTGRES", "results_postgres")
# Establecer la URL de los datos directamente
DATA_URL = "https://raw.githubusercontent.com/Iker186/streamlit-social-media/refs/heads/main/results/processed_data.json/part-00000-b71226d5-3187-479e-888f-23897cd4299a-c000.json"

def delivery_report(err, msg):
    if err:
        logging.error(f"Error al enviar mensaje: {err}")
    else:
        logging.info(f"Mensaje enviado a {msg.topic()}: {msg.value().decode('utf-8')}")

@app.route("/send-to-kafka-postgres", methods=["POST"])
def send_data():
    try:
        logging.info(f"Descargando datos desde: {DATA_URL}")
        response = requests.get(DATA_URL)
        response.raise_for_status()

        lines = response.text.strip().splitlines()
        logging.info(f"Total de registros a enviar: {len(lines)}")

        for line in lines:
            producer.produce(TOPIC, line.encode("utf-8"), callback=delivery_report)

        producer.flush()
        logging.info("Todos los datos fueron enviados correctamente.")
        return jsonify({"status": "success", "message": f"Datos enviados al tópico '{TOPIC}'"}), 200

    except Exception as e:
        logging.error("Error al enviar los datos", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/health")
def health():
    return "ok", 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))  # Render usa $PORT
    app.run(host="0.0.0.0", port=port)
