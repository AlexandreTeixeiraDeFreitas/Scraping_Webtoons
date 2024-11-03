import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from pymongo import MongoClient
from hdfs import InsecureClient
import random

# Définir la source de données ("mongo" ou "hdfs")
source = "hdfs"  # Remplacez par "hdfs" pour utiliser HDFS

# Charger les données depuis MongoDB
def load_data_from_mongo():
    MONGO_URI = "mongodb://mongodb:27017"
    client = MongoClient(MONGO_URI)
    db = client["webtoons"]
    collection = db["webtoon_data"]
    data = list(collection.find({}, {"views": 1, "subscribers": 1, "episodes.like_count": 1, "rating": 1}))
    return pd.DataFrame(data)

# Charger les données depuis HDFS
def load_data_from_hdfs():
    hdfs_client = InsecureClient("http://namenode:9870", user="hdfs")
    with hdfs_client.read("/webtoons_data/webtoon_data.json", encoding="utf-8") as reader:
        data = pd.read_json(reader, lines=True)  # Activer la lecture ligne par ligne pour JSON en lignes
    return data

# Sélectionner la source de données
if source == "mongo":
    df = load_data_from_mongo()
elif source == "hdfs":
    df = load_data_from_hdfs()
else:
    raise ValueError("Source de données non valide. Utilisez 'mongo' ou 'hdfs'.")

# Fonction de conversion pour les champs numériques
def convert_views(view_str):
    if isinstance(view_str, str):
        view_str = view_str.replace('\xa0', ' ').replace(' ', '').strip()
        if 'M' in view_str:
            return int(float(view_str.replace('M', '').replace(',', '.')) * 1e6)
        elif 'K' in view_str:
            return int(float(view_str.replace('K', '').replace(',', '.')) * 1e3)
        else:
            return int(view_str.replace(',', ''))
    return view_str

def convert_subscribers(subscriber_str):
    if isinstance(subscriber_str, str):
        subscriber_str = subscriber_str.replace('\xa0', ' ').replace(' ', '').strip()
        return int(subscriber_str)
    return subscriber_str

def convert_rating(rating_str):
    if isinstance(rating_str, str):
        return float(rating_str.replace(',', '.').strip())
    return rating_str

# Appliquer les conversions
df["views"] = df["views"].apply(convert_views)
df["subscribers"] = df["subscribers"].apply(convert_subscribers)
df["rating"] = df["rating"].apply(convert_rating)

# Calculer le like_count moyen pour les épisodes
df["like_count"] = df["episodes"].apply(lambda x: np.mean([ep["like_count"] for ep in x]) if x else 0)
print('affichage du calcul', df["like_count"])

# Sélectionner les colonnes d'intérêt
df = df[["views", "subscribers", "like_count", "rating"]].dropna()

# Séparer les features (X) et la cible (y)
X = df[["views", "subscribers", "like_count"]]
y = df["rating"]

# Diviser les données en ensembles d'entraînement et de test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Entraîner un modèle de régression linéaire
model = LinearRegression()
model.fit(X_train, y_train)

# Faire des prédictions sur l'ensemble de test
y_pred = model.predict(X_test)

# Calculer l'erreur quadratique moyenne
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse}")

# Afficher les coefficients du modèle
print("Coefficients:", model.coef_)
print("Intercept:", model.intercept_)

# Tester avec la deuxième ligne de la DataFrame
test_sample = X.iloc[2]
true_rating = y.iloc[2]
predicted_rating = model.predict([test_sample])

# Afficher les résultats
print("Test Sample (Features):", test_sample)
print("True Rating:", true_rating)
print("Predicted Rating:", predicted_rating[0])
