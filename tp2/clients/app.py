import os
import json
import time
import threading
from flask import Flask, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = "commandes-creees"
GROUP_ID = os.environ.get("GROUP_ID", "clients-group")

clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"}
]

# nombre de commandes par client
compteur = {}

def nom_client(client_id):
    for c in clients:
        if c["id"] == client_id:
            return c["nom"]
    return f"client inconnu ({client_id})"

def creer_consommateur():
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="earliest"
            )
        except Exception as e:
            print("Kafka pas encore disponible, nouvel essai dans 3 s...", e, flush=True)
            time.sleep(3)

def ecouter_kafka():
    consumer = creer_consommateur()
    print(f"Consommateur connecte (groupe {GROUP_ID})", flush=True)
    for message in consumer:
        try:
            evt = json.loads(message.value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            print("Message ignore (pas du JSON) :", message.value, flush=True)
            continue

        client_id = evt["client_id"]
        compteur[client_id] = compteur.get(client_id, 0) + 1

        print(f"[partition {message.partition}, offset {message.offset}] "
              f"Client : {nom_client(client_id)} | Produit : {evt['produit']} | "
              f"Montant : {evt['montant']}", flush=True)
        print("Commandes par client :",
              {nom_client(k): v for k, v in compteur.items()}, flush=True)

@app.route("/clients", methods=["GET"])
def get_clients():
    return jsonify(clients)

@app.route("/clients/<int:client_id>", methods=["GET"])
def get_client(client_id):
    for c in clients:
        if c["id"] == client_id:
            return jsonify(c)
    return jsonify({"erreur": "Client introuvable"}), 404

@app.route("/clients/compteurs", methods=["GET"])
def get_compteurs():
    return jsonify({nom_client(k): v for k, v in compteur.items()})

if __name__ == "__main__":
    threading.Thread(target=ecouter_kafka, daemon=True).start()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)