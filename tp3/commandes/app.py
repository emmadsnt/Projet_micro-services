import os
import json
import time
import threading
from datetime import date
from flask import Flask, jsonify, request
from kafka import KafkaProducer, KafkaConsumer

app = Flask(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC_COMMANDES = "commandes-creees"
TOPIC_DEMANDES = "demandes-prediction"
TOPIC_RESULTATS = "resultats-predictions"

CHAMPS_PREDICTION = ["client_id", "age", "anciennete", "nombre_commandes",
                     "montant_total", "montant_moyen", "jours_depuis_derniere_commande"]

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80}
]

# derniers résultats de prédiction reçus, par client
resultats = {}

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

def ecouter_resultats():
    consumer = attendre_kafka(lambda: KafkaConsumer(
        TOPIC_RESULTATS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="commandes-group",
        auto_offset_reset="earliest"
    ))
    print("En attente des resultats de prediction", flush=True)
    for message in consumer:
        evt = json.loads(message.value.decode("utf-8"))
        resultats[evt["client_id"]] = evt
        print(f"Resultat recu -> Client : {evt['client_id']} | "
              f"Prediction : {evt['prediction']} | "
              f"Probabilite : {round(evt['probabilite_commande'], 2)}", flush=True)

@app.route("/commandes", methods=["GET"])
def get_commandes():
    return jsonify(commandes)

@app.route("/commandes/<int:commande_id>", methods=["GET"])
def get_commande(commande_id):
    for c in commandes:
        if c["id"] == commande_id:
            return jsonify(c)
    return jsonify({"erreur": "Commande introuvable"}), 404

@app.route("/commandes", methods=["POST"])
def creer_commande():
    data = request.get_json()
    if not data or not all(k in data for k in ("client_id", "produit", "montant")):
        return jsonify({"erreur": "client_id, produit et montant sont obligatoires"}), 400
    commande = {
        "id": len(commandes) + 1,
        "client_id": data["client_id"],
        "produit": data["produit"],
        "montant": data["montant"],
        "date": data.get("date", date.today().isoformat())
    }
    commandes.append(commande)
    evenement = {
        "commande_id": commande["id"],
        "client_id": commande["client_id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "date": commande["date"]
    }
    producer.send(TOPIC_COMMANDES, key=commande["client_id"], value=evenement)
    producer.flush()
    return jsonify(commande), 201

# 3.3 envoyer une demande de prédiction
@app.route("/predictions", methods=["POST"])
def demander_prediction():
    data = request.get_json()
    if not data or not all(k in data for k in CHAMPS_PREDICTION):
        return jsonify({"erreur": "champs obligatoires : " + ", ".join(CHAMPS_PREDICTION)}), 400
    evenement = {k: data[k] for k in CHAMPS_PREDICTION}
    producer.send(TOPIC_DEMANDES, key=data["client_id"], value=evenement)
    producer.flush()
    print("Demande de prediction envoyee :", evenement, flush=True)
    return jsonify({"message": "demande envoyee", "client_id": data["client_id"]}), 202

@app.route("/predictions", methods=["GET"])
def get_predictions():
    return jsonify(list(resultats.values()))

if __name__ == "__main__":
    threading.Thread(target=ecouter_resultats, daemon=True).start()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)