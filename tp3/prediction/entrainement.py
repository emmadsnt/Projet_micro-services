import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# données historiques simulées (le sujet n'en fournit pas)
rng = np.random.default_rng(42)
n = 2000

nombre_commandes = rng.integers(0, 31, n)
montant_moyen = rng.uniform(10, 300, n).round(2)

df = pd.DataFrame({
    "age": rng.integers(18, 71, n),
    "anciennete": rng.integers(0, 11, n),
    "nombre_commandes": nombre_commandes,
    "montant_total": (nombre_commandes * montant_moyen).round(2),
    "montant_moyen": montant_moyen,
    "jours_depuis_derniere_commande": rng.integers(0, 366, n)
})

# règle utilisée pour fabriquer la cible, avec du bruit
score = (0.2 * df["nombre_commandes"]
         + 0.3 * df["anciennete"]
         - 0.02 * df["jours_depuis_derniere_commande"]
         + rng.normal(0, 1, n))
y = (score > 1).astype(int)

X_train, X_test, y_train, y_test = train_test_split(df, y, test_size=0.2, random_state=42)

model = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42)
model.fit(X_train, y_train)

print("Taille du jeu d'entrainement :", len(X_train))
print("Proportion de clients qui commandent :", round(y.mean(), 2))
print("Accuracy sur le jeu de test :", round(accuracy_score(y_test, model.predict(X_test)), 3))

joblib.dump(model, "modele_commande.pkl")
print("Modele sauvegarde dans modele_commande.pkl")

# test sur l'exemple du sujet
exemple = pd.DataFrame([{
    "age": 32, "anciennete": 3, "nombre_commandes": 8,
    "montant_total": 1250, "montant_moyen": 156.25,
    "jours_depuis_derniere_commande": 12
}])
print("Exemple du sujet -> prediction :", model.predict(exemple)[0],
      "| probabilite :", round(model.predict_proba(exemple)[0][1], 2))