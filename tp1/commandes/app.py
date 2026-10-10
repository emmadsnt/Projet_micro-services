import os
import requests
from flask import Flask, jsonify

app = Flask(__name__)

# Adresse du service clients (modifiable via une variable d'environnement)
CLIENTS_URL = os.environ.get("CLIENTS_URL", "http://localhost:8080")

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80}
]

@app.route("/commandes", methods=["GET"])
def get_commandes():
    return jsonify(commandes)

@app.route("/commandes/<int:commande_id>", methods=["GET"])
def get_commande(commande_id):
    # 1. Récupérer la commande
    commande = next((c for c in commandes if c["id"] == commande_id), None)
    if commande is None:
        return jsonify({"erreur": "Commande introuvable"}), 404

    # 2-3. Appeler le service clients avec le client_id
    try:
        r = requests.get(f"{CLIENTS_URL}/clients/{commande['client_id']}", timeout=3)
    except requests.exceptions.RequestException:
        return jsonify({"erreur": "Service clients indisponible"}), 503

    if r.status_code == 404:
        return jsonify({"erreur": "Client introuvable"}), 404

    # 4-5. Construire la réponse enrichie
    return jsonify({
        "id": commande["id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "client": r.json()
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)