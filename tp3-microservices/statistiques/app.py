from flask import Flask, jsonify
from kafka import KafkaConsumer
from collections import defaultdict
import json
import threading

app = Flask(__name__)

# Nombre de commandes par produit
commandes_par_produit = defaultdict(int)

# Chiffre d'affaires par mois
chiffre_affaires_par_mois = defaultdict(float)

# Nombre total de commandes consommées
nombre_total_commandes = 0


def consommer_commandes():
    global nombre_total_commandes

    print("Démarrage du consommateur Statistiques...", flush=True)

    try:
        consumer = KafkaConsumer(
            "commandes-creees",
            bootstrap_servers=["kafka:9092"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="statistiques-group",
            value_deserializer=lambda message: json.loads(
                message.decode("utf-8")
            )
        )

        print("Consommateur Statistiques connecté à Kafka.", flush=True)
        print("En attente de nouvelles commandes...", flush=True)

        for message in consumer:
            commande = message.value

            produit = commande["produit"]
            montant = float(commande["montant"])
            date = commande.get("date", "")

            # Compter les commandes par produit
            commandes_par_produit[produit] += 1
            nombre_total_commandes += 1

            # Calculer le chiffre d'affaires par mois
            # Format de date attendu : YYYY-MM-DD
            if len(date) >= 7 and date[4] == "-":
                mois = date[:7]
                chiffre_affaires_par_mois[mois] += montant

            print("\nNouvelle commande analysée !", flush=True)
            print(f"Produit : {produit}", flush=True)
            print(
                f"Nombre de commandes pour ce produit : "
                f"{commandes_par_produit[produit]}",
                flush=True
            )

            if len(date) >= 7 and date[4] == "-":
                print(
                    f"Chiffre d'affaires de {mois} : "
                    f"{chiffre_affaires_par_mois[mois]:.2f} €",
                    flush=True
                )

    except Exception:
        app.logger.exception("Erreur du consommateur Kafka")


# API REST : nombre de commandes pour un produit
@app.route("/statistiques/produits/<path:produit>", methods=["GET"])
def get_statistiques_produit(produit):
    return jsonify({
        "produit": produit,
        "nombre_commandes": commandes_par_produit.get(produit, 0)
    })


# API REST : chiffre d'affaires pour un mois
@app.route("/statistiques/chiffre-affaires/<mois>", methods=["GET"])
def get_chiffre_affaires(mois):
    if (
        len(mois) != 7
        or mois[4] != "-"
        or not mois[:4].isdigit()
        or not mois[5:].isdigit()
        or not 1 <= int(mois[5:]) <= 12
    ):
        return jsonify({
            "error": "Le mois doit être au format YYYY-MM"
        }), 400

    return jsonify({
        "mois": mois,
        "chiffre_affaires": round(
            chiffre_affaires_par_mois.get(mois, 0.0), 2
        )
    })


# API REST : résumé global
@app.route("/statistiques", methods=["GET"])
def get_statistiques():
    return jsonify({
        "nombre_total_commandes": nombre_total_commandes,
        "commandes_par_produit": dict(commandes_par_produit),
        "chiffre_affaires_par_mois": dict(chiffre_affaires_par_mois)
    })


if __name__ == "__main__":
    thread = threading.Thread(
        target=consommer_commandes,
        daemon=True
    )
    thread.start()

    app.run(host="0.0.0.0", port=8080)