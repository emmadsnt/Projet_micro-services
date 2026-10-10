import os
import json
import time
import random
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = "demandes-prediction"
DELAI = float(os.environ.get("DELAI", "0.5"))  # secondes entre deux demandes

def attendre_kafka(creer):
    while True:
        try:
            return creer()
        except Exception as e:
            print("Kafka pas encore disponible, nouvel essai dans 3 s...", e, flush=True)
            time.sleep(3)

producer = attendre_kafka(lambda: KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
))

compteur = 0
while True:
    nombre_commandes = random.randint(0, 30)
    montant_moyen = round(random.uniform(10, 300), 2)
    demande = {
        "client_id": random.randint(1, 1000),
        "age": random.randint(18, 70),
        "anciennete": random.randint(0, 10),
        "nombre_commandes": nombre_commandes,
        "montant_total": round(nombre_commandes * montant_moyen, 2),
        "montant_moyen": montant_moyen,
        "jours_depuis_derniere_commande": random.randint(0, 365)
    }
    producer.send(TOPIC, key=demande["client_id"], value=demande)
    compteur += 1
    if compteur % 20 == 0:
        print(f"{compteur} demandes envoyees", flush=True)
    time.sleep(DELAI)