import paho.mqtt.client as mqtt
import json
import time
import random
from datetime import datetime
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration des capteurs simulés
MACHINES = ["PUMP001", "PUMP002", "PUMP003", "REACTOR001", "REACTOR002"]
TYPES_CAPTEURS = ["temperature", "vibration"]

class SimulateurCapteurs:
    def __init__(self):
        self.client = mqtt.Client()
        
    def connecter(self):
        try:
            self.client.connect("localhost", 1883, 60)
            logger.info("Connecté au broker MQTT")
        except Exception as e:
            logger.error("Erreur de connexion: %s", str(e))
            raise

    def generer_valeur(self, type_capteur):
        if type_capteur == "temperature":
            # Température normale entre 60 et 120°C avec possibilité de valeurs anormales
            return random.uniform(30, 150)
        else:  # vibration
            # Vibrations normales entre 0.5 et 2.5 mm/s avec possibilité de valeurs anormales
            return random.uniform(0, 5)

    def simuler(self):
        while True:
            try:
                for machine in MACHINES:
                    for type_capteur in TYPES_CAPTEURS:
                        donnees = {
                            "machine_id": machine,
                            "type_capteur": type_capteur,
                            "valeur": self.generer_valeur(type_capteur),
                            "timestamp": datetime.now().isoformat()
                        }
                        
                        # Publication sur le topic MQTT
                        topic = f"sensors/{machine}/{type_capteur}"
                        self.client.publish(topic, json.dumps(donnees))
                        logger.info("Données publiées sur %s: %s", topic, donnees)
                
                # Attendre 5 secondes avant la prochaine série de mesures
                time.sleep(5)
                
            except Exception as e:
                logger.error("Erreur lors de la simulation: %s", str(e))
                time.sleep(5)  # Attendre avant de réessayer en cas d'erreur

def main():
    simulateur = SimulateurCapteurs()
    simulateur.connecter()
    simulateur.simuler()

if __name__ == "__main__":
    main()