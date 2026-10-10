import os
import json
import time
import threading
from flask import Flask, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = "commandes-creees"
GROUP_ID = os.environ.get("GROUP_ID", "statistiques-group")

commandes_par_produit = {}
ca_par_mois = {}

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

        produit = evt["produit"]
        commandes_par_produit[produit] = commandes_par_produit.get(produit, 0) + 1

        # les anciens événements n'ont pas de date
        mois = evt["date"][:7] if "date" in evt else "sans date"
        ca_par_mois[mois] = ca_par_mois.get(mois, 0) + evt["montant"]

        print(f"[partition {message.partition}, offset {message.offset}] "
              f"{produit} | {evt['montant']} | mois : {mois}", flush=True)
        print("Commandes par produit :", commandes_par_produit, flush=True)
        print("CA par mois :", ca_par_mois, flush=True)

@app.route("/stats/produits/<produit>", methods=["GET"])
def stats_produit(produit):
    return jsonify({"produit": produit,
                    "nombre_commandes": commandes_par_produit.get(produit, 0)})

@app.route("/stats/mois/<mois>", methods=["GET"])
def stats_mois(mois):
    return jsonify({"mois": mois, "chiffre_affaires": ca_par_mois.get(mois, 0)})

@app.route("/stats", methods=["GET"])
def stats():
    return jsonify({"commandes_par_produit": commandes_par_produit,
                    "ca_par_mois": ca_par_mois})

if __name__ == "__main__":
    threading.Thread(target=ecouter_kafka, daemon=True).start()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)