from flask import Flask, jsonify
from kafka import KafkaConsumer
import json
import threading

app = Flask(__name__)

clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"}
]

# Compteur du nombre de commandes par client
commandes_par_client = {}


# Route REST : récupérer tous les clients
@app.route("/clients", methods=["GET"])
def get_clients():
    return jsonify(clients)


# Route REST : récupérer un client par son identifiant
@app.route("/clients/<int:id>", methods=["GET"])
def get_client(id):
    for client in clients:
        if client["id"] == id:
            return jsonify(client)

    return jsonify({"error": "Client non trouvé"}), 404


# Consommateur Kafka
def consommer_commandes():
    print("Démarrage du consommateur Kafka...", flush=True)

    try:
        consumer = KafkaConsumer(
            "commandes-creees",
            bootstrap_servers=["kafka:9092"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="clients-group",
            value_deserializer=lambda message: json.loads(
                message.decode("utf-8")
            )
        )

        print("Consommateur Clients connecté à Kafka.", flush=True)
        print("En attente de nouvelles commandes...", flush=True)

        for message in consumer:
            commande = message.value

            client_id = commande["client_id"]
            produit = commande["produit"]
            montant = commande["montant"]

            client = next(
                (c for c in clients if c["id"] == client_id),
                None
            )

            nom_client = (
                client["nom"]
                if client is not None
                else f"Client inconnu (ID {client_id})"
            )

            commandes_par_client[client_id] = (
                commandes_par_client.get(client_id, 0) + 1
            )

            print("\nNouvelle commande reçue !", flush=True)
            print(f"Client : {nom_client}", flush=True)
            print(f"Produit : {produit}", flush=True)
            print(f"Montant : {montant} €", flush=True)
            print(
                f"Nombre total de commandes du client {client_id} : "
                f"{commandes_par_client[client_id]}",
                flush=True
            )

    except Exception:
        app.logger.exception("Erreur du consommateur Kafka")

# Démarrer le consommateur dans un thread séparé
def demarrer_consommateur():
    thread = threading.Thread(
        target=consommer_commandes,
        daemon=True
    )
    thread.start()


if __name__ == "__main__":
    demarrer_consommateur()
    app.run(host="0.0.0.0", port=8080)