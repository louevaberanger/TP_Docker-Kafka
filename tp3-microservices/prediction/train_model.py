import numpy as np
import pandas as pd
import joblib

from pathlib import Path
from sklearn.ensemble import RandomForestClassifier

# Dossier dans lequel se trouve ce script
BASE_DIR = Path(__file__).resolve().parent

# Pour obtenir les mêmes données à chaque exécution
rng = np.random.default_rng(42)

# Nombre de clients simulés pour entraîner le modèle
n = 1000

# Création d'un jeu de données fictif
df = pd.DataFrame({
    "age": rng.integers(18, 80, n),
    "anciennete": rng.integers(0, 15, n),
    "nombre_commandes": rng.integers(0, 40, n),
    "montant_total": rng.uniform(0, 5000, n).round(2),
    "montant_moyen": rng.uniform(10, 400, n).round(2),
    "jours_depuis_derniere_commande": rng.integers(0, 365, n)
})

# Création d'une cible fictive :
# 1 = le client est susceptible de commander
# 0 = le client est moins susceptible de commander
score = (
    df["nombre_commandes"] * 0.8
    + df["anciennete"] * 0.3
    + df["montant_total"] / 400
    - df["jours_depuis_derniere_commande"] * 0.04
    + rng.normal(0, 3, n)
)

df["commande"] = (score > np.median(score)).astype(int)

# Variables utilisées par le modèle
features = [
    "age",
    "anciennete",
    "nombre_commandes",
    "montant_total",
    "montant_moyen",
    "jours_depuis_derniere_commande"
]

X = df[features]
y = df["commande"]

# Création et entraînement du modèle
model = RandomForestClassifier(
    n_estimators=100,
    random_state=42
)

model.fit(X, y)

# Sauvegarde du modèle entraîné
model_path = BASE_DIR / "modele_commande.pkl"
joblib.dump(model, model_path)

print("Modèle entraîné avec succès !")
print(f"Modèle sauvegardé dans : {model_path}")