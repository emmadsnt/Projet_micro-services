import os
import json
import time
import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC_DEMANDES = "demandes-prediction"
TOPIC_RESULTATS = "resultats-predictions"
GROUP_ID = os.environ.get("GROUP_ID", "prediction-group")
INSTANCE = os.environ.get("HOSTNAME", "?")

COLONNES = ["age", "anciennete", "nombre_commandes", "montant_total",
            "montant_moyen", "jours_depuis_derniere_commande"]

# le modèle est chargé une seule fois au démarrage
model = joblib.load("modele_commande.pkl")
print("Modele charge", flush=True)

def attendre_kafka(creer):
    while True:
        try:
            return creer()
        except Exception as e:
            print("Kafka pas encore disponible, nouvel essai dans 3 s...", e, flush=True)
            time.sleep(3)

consumer = attendre_kafka(lambda: KafkaConsumer(
    TOPIC_DEMANDES,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest"
))
producer = attendre_kafka(lambda: KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
))
print(f"Instance {INSTANCE} prete (groupe {GROUP_ID})", flush=True)

for message in consumer:
    try:
        data = json.loads(message.value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        print("Message ignore (pas du JSON)", flush=True)
        continue

    manquants = [c for c in COLONNES + ["client_id"] if c not in data]
    if manquants:
        print("Message ignore, champs manquants :", manquants, flush=True)
        continue

    # 3.4 construire l'entrée du modèle
    X = pd.DataFrame([{c: data[c] for c in COLONNES}])

    # 3.5 prédiction
    prediction = model.predict(X)[0]
    probabilite = model.predict_proba(X)[0][1]

    print(f"[instance {INSTANCE}, partition {message.partition}]", flush=True)
    print("Client :", data["client_id"], flush=True)
    print("Prediction :", prediction, flush=True)
    print("Probabilite :", round(probabilite, 2), flush=True)

    # 3.6 publier le résultat
    resultat = {
        "client_id": data["client_id"],
        "prediction": int(prediction),
        "probabilite_commande": float(probabilite)
    }
    producer.send(TOPIC_RESULTATS, key=data["client_id"], value=resultat)
    producer.flush()