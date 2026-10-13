from flask import Flask, jsonify
import joblib
import json
import threading

import pandas as pd
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer

app = Flask(__name__)

# --------------------------------------------------
# 1. Chargement du modèle de Machine Learning
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
MODEL_PATH = BASE_DIR / "modele_commande.pkl"

model = joblib.load(MODEL_PATH)

print("Modèle de prédiction chargé avec succès !", flush=True)


# --------------------------------------------------
# 2. Connexion à Kafka pour recevoir les commandes
# --------------------------------------------------

def consommer_commandes():

    print("Démarrage du consommateur Kafka...", flush=True)

    consumer = KafkaConsumer(
        "commandes-creees",
        bootstrap_servers=["kafka:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="prediction-group",
        value_deserializer=lambda message: json.loads(
            message.decode("utf-8")
        )
    )

    # Producteur pour publier les résultats
    producer = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    print("Connecté à Kafka !", flush=True)
    print("En attente de commandes...", flush=True)

    # --------------------------------------------------
    # 3. Traitement des événements reçus
    # --------------------------------------------------

    for message in consumer:

        data = message.value

        print("\nNouvel événement reçu !", flush=True)
        print(f"Client : {data['client_id']}", flush=True)

        # Construction des caractéristiques du client
        features = [
            "age",
            "anciennete",
            "nombre_commandes",
            "montant_total",
            "montant_moyen",
            "jours_depuis_derniere_commande"
        ]

        X = pd.DataFrame(
            [[data[feature] for feature in features]],
            columns=features
        )

        # --------------------------------------------------
        # 4. Prédiction avec le modèle ML
        # --------------------------------------------------

        prediction = model.predict(X)[0]

        probabilite = model.predict_proba(X)[0][1]

        print(f"Prédiction : {prediction}", flush=True)
        print(
            f"Probabilité de commande : {probabilite:.2%}",
            flush=True
        )

        # --------------------------------------------------
        # 5. Création de l'événement de prédiction
        # --------------------------------------------------

        resultat = {
            "client_id": data["client_id"],
            "prediction": int(prediction),
            "probabilite_commande": float(probabilite)
        }

        # Publication dans le topic Kafka predictions
        producer.send("predictions", value=resultat).get(timeout=10)

        print(
            f"Résultat publié dans Kafka : {resultat}",
            flush=True
        )


# --------------------------------------------------
# 6. Route HTTP pour vérifier le service
# --------------------------------------------------

@app.route("/", methods=["GET"])
def accueil():
    return jsonify({
        "message": "Microservice Prediction opérationnel"
    })


if __name__ == "__main__":

    thread = threading.Thread(
        target=consommer_commandes,
        daemon=True
    )
    thread.start()

    app.run(host="0.0.0.0", port=8080)