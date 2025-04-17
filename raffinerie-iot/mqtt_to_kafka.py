import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Callback lors de la connexion au broker MQTT
def on_connect(client, userdata, flags, rc):
    logger.info("Connecté au broker MQTT avec le code: %s", str(rc))
    client.subscribe("sensors/#")

# Callback lors de la réception d'un message MQTT
def on_message(client, userdata, msg):
    try:
        # Décoder le message JSON
        payload = json.loads(msg.payload.decode())
        logger.info("Message reçu sur %s: %s", msg.topic, payload)
        
        # Envoyer le message à Kafka
        producer.send('sensor-data', value=payload)
        producer.flush()
        
        logger.info("Message transmis à Kafka avec succès")
    except Exception as e:
        logger.error("Erreur lors du traitement du message: %s", str(e))

def main():
    # Création du client MQTT
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        # Connexion au broker MQTT
        client.connect("mqtt", 1883, 60)
        logger.info("Démarrage de la boucle MQTT...")
        client.loop_forever()
    except Exception as e:
        logger.error("Erreur de connexion MQTT: %s", str(e))
        
if __name__ == "__main__":
    main()