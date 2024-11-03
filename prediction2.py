import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from pymongo import MongoClient
from hdfs import InsecureClient

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

# Créer une colonne binaire pour la présence de qr_code
df["has_qr_code"] = df["qr_code"].apply(lambda x: 1 if x else 0)

# Combiner les titres des épisodes et descriptions des auteurs en une seule chaîne par webtoon
df["episode_titles"] = df["episodes"].apply(lambda x: " ".join([ep["episode_title"] for ep in x]) if x else "")
df["author_description"] = df["authors"].apply(lambda authors: " ".join([author.get("description", "") for author in authors]) if authors else "")

# Remplir les valeurs manquantes par des valeurs par défaut
df["genre"] = df["genre"].fillna("")
df["summary"] = df["summary"].fillna("")
df["author_description"] = df["author_description"].fillna("")
df["title"] = df["title"].fillna("")

# Sélection des features et de la cible
X = df[["summary", "author_description", "genre", "title", "has_qr_code"]]
y = df["rating"]

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Pipeline pour les données numériques, textuelles, et catégorielles
preprocessor = ColumnTransformer(
    transformers=[
        ("summary_vectorizer", TfidfVectorizer(max_features=100, stop_words='english'), "summary"),
        ("author_description_vectorizer", TfidfVectorizer(max_features=100, stop_words='english'), "author_description"),
        ("title_vectorizer", TfidfVectorizer(max_features=100, stop_words='english'), "title"),
        ("genre_encoder", OneHotEncoder(handle_unknown='ignore'), ["genre"]),
        ("has_qr_code_scaler", StandardScaler(), ["has_qr_code"])
    ]
)

# Modèle de régression avec Pipeline
pipeline = Pipeline(steps=[
    ("preprocessor", preprocessor),
    ("regressor", RandomForestRegressor(n_estimators=100, random_state=42))
])

# Entraînement du modèle
pipeline.fit(X_train, y_train)

# Prédiction et évaluation
y_pred = pipeline.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse}")

# Tester sur un échantillon
test_sample = pd.DataFrame([X.iloc[2]])  # Convertir en DataFrame pour le prédicteur
true_rating = y.iloc[2]
predicted_rating = pipeline.predict(test_sample)

# Afficher les résultats
print("Test Sample (Features):", test_sample)
print("True Rating:", true_rating)
print("Predicted Rating:", predicted_rating[0])
