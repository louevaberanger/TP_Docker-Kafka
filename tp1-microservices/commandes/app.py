from flask import Flask, jsonify
import requests

app = Flask(__name__)

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80}
]


@app.route("/commandes", methods=["GET"])
def get_commandes():
    return jsonify(commandes)


@app.route("/commandes/<int:id>", methods=["GET"])
def get_commande(id):

    # 1. Rechercher la commande
    commande = next(
        (c for c in commandes if c["id"] == id),
        None
    )

    # 2. Vérifier que la commande existe
    if commande is None:
        return jsonify({
            "error": "Commande non trouvée"
        }), 404

    # 3. Récupérer le client_id
    client_id = commande["client_id"]

    # 4. Appeler le microservice Clients
    try:
        response = requests.get(
            f"http://clients:8080/clients/{client_id}",
            timeout=5
        )

    except requests.RequestException:
        return jsonify({
            "error": "Le microservice Clients est indisponible"
        }), 502

    # 5. Gérer le cas où le client n'existe pas
    if response.status_code == 404:
        return jsonify({
            "error": "Client associé à la commande non trouvé"
        }), 404

    # 6. Gérer les autres erreurs du microservice Clients
    if response.status_code != 200:
        return jsonify({
            "error": "Erreur lors de la récupération du client"
        }), 502

    # 7. Récupérer les informations du client
    client = response.json()

    # 8. Construire la réponse finale
    resultat = {
        "id": commande["id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "client": client
    }

    return jsonify(resultat)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)