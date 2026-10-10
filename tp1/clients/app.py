from flask import Flask, jsonify

app = Flask(__name__)

clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"}
]

@app.route("/clients", methods=["GET"])
def get_clients():
    return jsonify(clients)

@app.route("/clients/<int:client_id>", methods=["GET"])
def get_client(client_id):
    for client in clients:
        if client["id"] == client_id:
            return jsonify(client)
    return jsonify({"erreur": "Client introuvable"}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)