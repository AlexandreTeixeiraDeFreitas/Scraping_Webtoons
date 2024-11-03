Voici une mise à jour du README avec une section dédiée à l'intégration de la prédiction IA :

---

# Webtoon Scraper

## Description
Le projet **Webtoon Scraper** est conçu pour extraire des informations et des commentaires de la plateforme de webtoons à partir d'URL de genres spécifiques, en stockant les données dans une base MongoDB et en les exportant vers un cluster Hadoop via HDFS. Le projet utilise BeautifulSoup et Selenium pour naviguer dans les pages et récupérer le contenu, y compris les auteurs et les descriptions des webtoons, ainsi que les commentaires des utilisateurs. Ce processus est planifié pour s'exécuter automatiquement à intervalles réguliers.

Une fonctionnalité IA a été intégrée pour prédire le rating des webtoons en fonction de leurs vues, abonnés et likes d’épisodes, offrant une estimation de popularité grâce à un modèle de régression linéaire entraîné sur ces données.

## Fonctionnalités
- **Extraction d'informations détaillées sur les webtoons** : titre, auteurs, description, note, nombre de vues, abonnés, etc.
- **Collecte des commentaires des épisodes** : y compris le nombre de likes/dislikes, les réponses et les informations sur les utilisateurs.
- **Prédiction du rating par IA** : modèle de régression linéaire entraîné pour prédire la note des webtoons en fonction des données de popularité.
- **Stockage dans MongoDB** : permet un accès rapide aux données extraites.
- **Exportation vers HDFS** : transfère les données pour des analyses à grande échelle dans un environnement Hadoop.
- **Planification des tâches avec APScheduler** : assure une exécution régulière du scraping et du transfert de données.

## Structure du Projet

### Fichiers et Répertoires Principaux
- `app/`: Contient le code source principal du projet.
  - `script_scraping.py`: Script pour extraire les informations générales des webtoons.
  - `script_scraping_comment.py`: Script pour récupérer les commentaires des épisodes.
  - `main_scheduler.py`: Lanceur principal pour planifier les tâches d'extraction et de mise à jour des commentaires.
- `IA/`: Répertoire contenant le code pour la prédiction IA.
  - `prediction.py`: Code de la prédiction IA pour estimer le rating.
- `requirements.txt`: Liste des dépendances Python nécessaires.
- `docker-compose.yml`: Configuration Docker pour orchestrer les services (MongoDB, Selenium, Hadoop, IA, etc.)
- `README.md`: Documentation du projet (ce fichier).

## Prérequis
- **Python 3.11**
- **MongoDB** (local ou hébergé dans Docker)
- **Hadoop** avec un environnement HDFS accessible
- **Selenium avec Chrome** pour le scraping des commentaires des pages Web
- **Docker et Docker Compose** pour l'exécution de l'application dans des conteneurs

## Installation

### Cloner le dépôt
```bash
git clone <url_du_projet>
cd <nom_du_projet>
```

### Installation des dépendances Python
Assurez-vous d’avoir **Python 3.11** et **pip** installés. Installez les dépendances avec :
```bash
pip install -r requirements.txt
```

### Configuration des Variables d'Environnement
Créez un fichier de configuration pour définir les variables d'environnement, telles que :
```env
MONGO_URI=mongodb://mongodb:27017
HDFS_URL=http://namenode:9870
HDFS_DIR=/webtoons_data
USE_HDFS=True  # True pour activer le transfert vers HDFS
```

### Lancer avec Docker Compose
Assurez-vous d'avoir Docker et Docker Compose installés. Ensuite, lancez les services :
```bash
docker-compose up --build
```

### Structure du Réseau
Tous les conteneurs (MongoDB, Selenium, IA, etc.) sont configurés pour être sur le même réseau Docker, permettant une communication transparente.

## Utilisation

### Lancer le Scraping Manuellement
Pour lancer le scraping manuel depuis la ligne de commande, exécutez :

```bash
python app/script_scraping.py
```

Pour la collecte des commentaires :
```bash
python app/script_scraping_comment.py
```

### Lancer l’IA pour prédire le rating
Pour exécuter la prédiction de rating avec l'IA :
```bash
docker-compose run python-app-ia
```

### Scheduler Automatisé
La planification est gérée par APScheduler. Le fichier `main_scheduler.py` initialise un planificateur qui exécute :
- **L'extraction des données** toutes les 24 heures.
- **La mise à jour des commentaires** après chaque extraction.

## Exemples de Code

### Extraire des informations sur les auteurs
Ce code dans `script_scraping.py` extrait les noms et descriptions des auteurs d’un webtoon :
```python
try:
    authors_info = []
    author_sections = soup.select("div.ly_creator_in")
    
    for section in author_sections:
        name = section.select_one("h3.title").text.strip() if section.select_one("h3.title") else ""
        desc = section.select_one("p.desc").text.strip() if section.select_one("p.desc") else ""
        
        authors_info.append({"name": name, "description": desc})
    
    webtoon_info['authors'] = authors_info
except Exception as e:
    print(f"[Erreur] Impossible de récupérer les informations des auteurs: {e}")
```

### Planificateur APScheduler
Le planificateur dans `main_scheduler.py` assure l’exécution périodique :
```python
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler()

def run_extraction():
    # Code pour l'extraction

def run_comment_update(): 
    # Code pour la mise à jour des commentaires

scheduler.add_job(run_extraction, 'interval', hours=24)
scheduler.start()
```

## Prédiction IA avec le Modèle de Régression Linéaire
Le script `prediction.py` utilise un modèle de régression linéaire pour prédire le rating basé sur les vues, abonnés et likes d'épisodes. Les données sont extraites de MongoDB ou HDFS, traitées, et utilisées pour entraîner le modèle.

Exemple de code de prédiction :
```python
# Entraîner un modèle de régression linéaire
model = LinearRegression()
model.fit(X_train, y_train)

# Faire des prédictions sur l'ensemble de test
y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse}")

# Prédiction sur un exemple
test_sample = X.iloc[2]
predicted_rating = model.predict([test_sample])
print("Predicted Rating:", predicted_rating[0])
```

## Dépannage

### Note : 
La fonction `run_comment_update()` a été mise en commentaire dans `main_scheduler.py` car le script utilisant Selenium est très lent. Vous pouvez décider de l'activer si vous souhaitez exécuter cette étape malgré tout.

### Erreurs courantes
1. **Problèmes de connexion MongoDB** : Assurez-vous que l'URI MongoDB correspond à celui du conteneur MongoDB.
2. **Timeout Selenium** : Si le chargement des commentaires est lent, augmentez les délais dans `fetch_episode_comments` en ajustant `WebDriverWait`.
3. **Connexion HDFS** : Vérifiez que le cluster Hadoop est opérationnel et que les URL et chemins HDFS sont corrects.

## Contributeurs
- [Alexandre Teixeira]
- [Alexandre Da Silva]

---

Ce README est maintenant mis à jour pour inclure la section de prédiction IA avec des détails supplémentaires pour l’exécution et l’interprétation des résultats de la prédiction.
