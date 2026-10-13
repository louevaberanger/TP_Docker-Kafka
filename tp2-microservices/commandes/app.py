from flask import Flask, jsonify, request
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Connexion au broker Kafka
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Topic dans lequel publier les événements
TOPIC = "commandes-creees"

# Liste des commandes
commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80}
]


# Consulter toutes les commandes
@app.route("/commandes", methods=["GET"])
def get_commandes():
    return jsonify(commandes)


# Consulter une commande par son identifiant
@app.route("/commandes/<int:id>", methods=["GET"])
def get_commande(id):
    commande = next(
        (c for c in commandes if c["id"] == id),
        None
    )

    if commande is None:
        return jsonify({"error": "Commande non trouvée"}), 404

    return jsonify(commande)


# Créer une commande et publier un événement Kafka
@app.route("/commandes", methods=["POST"])
def creer_commande():
    donnees = request.get_json(silent=True)

    if not isinstance(donnees, dict):
        return jsonify({"error": "Un objet JSON est attendu"}), 400

    champs_requis = ["client_id", "produit", "montant", "date"]

    if any(champ not in donnees for champ in champs_requis):
        return jsonify({
            "error": "Champs requis : client_id, produit, montant, date"
        }), 400

    if (
        not isinstance(donnees["client_id"], int)
        or isinstance(donnees["client_id"], bool)
        or not isinstance(donnees["produit"], str)
        or not donnees["produit"].strip()
        or not isinstance(donnees["montant"], (int, float))
        or isinstance(donnees["montant"], bool)
        or donnees["montant"] < 0
        or not isinstance(donnees["date"], str)
        or not donnees["date"].strip()
    ):
        return jsonify({"error": "Valeurs invalides"}), 400

    # Générer un identifiant pour la nouvelle commande
    nouvel_id = max(c["id"] for c in commandes) + 1

    commande = {
        "id": nouvel_id,
        "client_id": donnees["client_id"],
        "produit": donnees["produit"],
        "montant": donnees["montant"],
        "date": donnees["date"]
    }

    # Enregistrer la commande en mémoire
    commandes.append(commande)

    # Construire l'événement à envoyer à Kafka
    evenement = {
        "commande_id": commande["id"],
        "client_id": commande["client_id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "date": commande["date"]
    }

    try:
        # Publier l'événement dans le topic Kafka
        producer.send(TOPIC, value=evenement).get(timeout=10)

    except Exception:
        app.logger.exception("Échec de publication dans Kafka")
        commandes.remove(commande)

        return jsonify({
            "error": "Impossible de publier l'événement dans Kafka"
        }), 503

    return jsonify({
        "message": "Commande créée et événement publié dans Kafka",
        "commande": commande
    }), 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)