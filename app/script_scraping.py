import asyncio
import requests
from bs4 import BeautifulSoup
import time
import re
import json
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from hdfs import InsecureClient
from datetime import datetime, timedelta

# Configuration MongoDB et HDFS
# MONGO_URI = "mongodb://localhost:27017"
MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "webtoons"
MONGO_COLLECTION = "webtoon_data"
MONGO_PROCESSED_URLS_COLLECTION = False #"processed_urls"
HDFS_URL = "http://namenode:9870"
HDFS_DIR = "/webtoons_data"
USE_HDFS = True

DAY_MAP = {
    "LUN": 0, "LUNDI": 0,
    "MAR": 1, "MARDI": 1,
    "MER": 2, "MERCREDI": 2,
    "JEU": 3, "JEUDI": 3,
    "VEN": 4, "VENDREDI": 4,
    "SAM": 5, "SAMEDI": 5,
    "DIM": 6, "DIMANCHE": 6
}

# Connexion MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    processed_urls_collection = db[MONGO_PROCESSED_URLS_COLLECTION] if MONGO_PROCESSED_URLS_COLLECTION else None
    print("[INFO] Connexion réussie à MongoDB")
except Exception as e:
    print(f"[ERREUR] Connexion échouée à MongoDB : {e}")

# Connexion HDFS
if USE_HDFS:
    try:
        hdfs_client = InsecureClient(HDFS_URL)
        if not hdfs_client.status(HDFS_DIR, strict=False):
            hdfs_client.makedirs(HDFS_DIR)
            print(f"[INFO] Répertoire {HDFS_DIR} créé dans HDFS.")
        print("[INFO] Connexion réussie à HDFS")
    except Exception as e:
        print(f"[ERREUR] Impossible de se connecter à HDFS : {e}")

# Fonction pour vérifier si un webtoon a déjà été traité en utilisant l'URL ou la date de mise à jour
def is_url_processed(url, last_update):
    try:
        # Vérifier l'existence de la collection processed_urls et si elle est utilisée
        if MONGO_PROCESSED_URLS_COLLECTION:
            # Vérifier dans la collection des URLs traitées
            return processed_urls_collection.find_one({"url": url}) is not None
        else:
            if last_update:
                # Convertir la date de dernière mise à jour en objet datetime pour la comparaison
                last_update_dt = datetime.strptime(last_update, "%Y-%m-%d")
                # Vérifier si la mise à jour a été faite aujourd'hui
                return last_update_dt.date() == datetime.today().date()
            else:
                # Si `last_update` est None, considérer que l'URL n'a pas été traitée
                return False
    except Exception as e:
        print(f"[ERREUR] Erreur lors de la vérification de l'URL traitée : {e}")
    return False

# Supprimer les URLs traitées à la fin de l'exécution
def clear_processed_urls():
    if processed_urls_collection:   
        processed_urls_collection.delete_many({})
        print("[INFO] Toutes les URLs traitées ont été supprimées de MongoDB.")

# Requête avec tentatives en cas d'erreur
async def fetch_with_retry_async(url, max_retries=5, delay=5):
    attempt = 0
    while attempt < max_retries:
        try:
            response = await asyncio.to_thread(requests.get, url)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"[ERREUR] Échec de connexion à {url}. Tentative {attempt + 1}/{max_retries}.")
            attempt += 1
            await asyncio.sleep(delay)
    return None

# Fonction pour insérer ou mettre à jour en batch dans MongoDB, avec ajout de `last_update`
def batch_upsert(data):
    try:
        for doc in data:
            doc["last_update"] = datetime.today().strftime("%Y-%m-%d")
            collection.update_one(
                {"title": doc["title"]},
                {"$set": doc},
                upsert=True
            )
        print(f"[INFO] Mise à jour de batch effectuée pour {len(data)} documents.")
    except BulkWriteError as e:
        print(f"[ERREUR] Erreur lors de la mise à jour en batch : {e.details}")
        for error in e.details['writeErrors']:
            print("Erreur de mise à jour:", error)

# Fonction pour vérifier la condition de mise à jour des webtoons en fonction de `day_info`
def should_update_webtoon(day_info, day_filter, last_update):
    # Si aucune `last_update` n'existe, il faut mettre à jour
    if not last_update:
        print(f"[DEBUG] Pas de `last_update` trouvée. Mise à jour requise pour day_info='{day_info}' et day_filter='{day_filter}'.")
        return True

    today = datetime.today()
    print(f"[DEBUG] Date actuelle: {today.strftime('%Y-%m-%d')}")

    # Si `day_info` est "TERMINÉ" ou `day_filter` contient "TERMINÉ", vérifier la date de dernière mise à jour
    if day_info == "TERMINÉ" or (day_filter and day_filter.upper() == "TERMINÉ"):
        last_update_dt = datetime.strptime(last_update, "%Y-%m-%d")
        days_since_last_update = (today - last_update_dt).days
        print(f"[DEBUG] Webtoon terminé. Dernière mise à jour : {last_update}. Jours écoulés : {days_since_last_update}")
        return days_since_last_update > 30

    # Vérifier le filtre de jour en priorité
    if day_filter:
        day_filter_num = DAY_MAP.get(day_filter.upper())
        if day_filter_num is not None:
            # Prioriser le `day_filter`
            print(f"[DEBUG] Priorité au filtre de jour ({day_filter}) : vérification pour jour {day_filter_num}.")
            return today.weekday() == day_filter_num

    # Extraire tous les jours de mise à jour indiqués dans `day_info`
    update_days = [DAY_MAP[day.strip()] for day in re.findall(r"LUN|MAR|MER|JEU|VEN|SAM|DIM", day_info)]
    print(f"[DEBUG] Jours de mise à jour extraits de `day_info`: {update_days}")
    
    # Vérifier si le jour actuel est un jour de mise à jour
    today_is_update_day = today.weekday() in update_days
    if today_is_update_day:
        print(f"[DEBUG] Aujourd'hui ({today.weekday()}) est un jour de mise à jour.")

    return today_is_update_day


# Récupérer tous les liens de pagination et les trier par numéro de page
async def get_all_pagination_links_async(base_url):
    pagination_links = set()
    queue = [f"{base_url}&page=1"]

    while queue:
        current_url = queue.pop(0)
        
        if current_url in pagination_links:
            continue
        
        response = await fetch_with_retry_async(current_url)
        if response is None:
            continue
        
        pagination_links.add(current_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        page_links = soup.select("div.paginate a")
        for link in page_links:
            page_url = link.get("href")
            if page_url and "page" in page_url:
                full_url = f"https://www.webtoons.com{page_url}"
                if full_url not in pagination_links:
                    queue.append(full_url)

    pagination_links = sorted(pagination_links, key=lambda url: int(re.search(r"page=(\d+)", url).group(1)))
    return pagination_links

# Récupérer les épisodes d'un webtoon
async def get_webtoon_episodes_async(webtoon_url, episode_limit=None):
    episodes = []
    count = 0
    pagination_links = await get_all_pagination_links_async(webtoon_url)
    
    for page_url in pagination_links:
        if episode_limit is not None and count >= episode_limit:
            print(f"[INFO] Limite d'épisodes atteinte ({episode_limit}).")
            return episodes
        
        response = await fetch_with_retry_async(page_url)
        if response is None:
            continue
        soup = BeautifulSoup(response.text, 'html.parser')
        
        episode_items = soup.select("ul#_listUl li._episodeItem")
        for episode in episode_items:
            if episode_limit is not None and count >= episode_limit:
                return episodes
            
            like_count_element = episode.select_one("span.like_area")
            like_count_text = like_count_element.text.strip() if like_count_element else "0"
            like_count = int(re.sub(r'[^\d]', '', like_count_text))
            
            episode_info = {
                'episode_title': episode.select_one("span.subj span").text.strip(),
                'date': episode.select_one("span.date").text.strip(),
                'like_count': like_count,
                'url': episode.find("a")["href"]
            }
            episodes.append(episode_info)
            count += 1
        
        print(f"[INFO] Episodes traités depuis {page_url}")
    
    return episodes

# Fonction pour récupérer les détails d'un webtoon de manière asynchrone
async def get_webtoon_details_async(webtoon_url):
    response = await fetch_with_retry_async(webtoon_url)
    if response is None:
        return {}
    soup = BeautifulSoup(response.text, 'html.parser')
    webtoon_info = {}

    try:
        webtoon_info['title'] = soup.select_one("h1.subj").text.strip() if soup.select_one("h1.subj") else ""
    except Exception as e:
        print(f"[Erreur] Impossible de récupérer le titre: {e}")
    
    try:
        webtoon_info['cover_image'] = soup.select_one("span.thmb img")['src'] if soup.select_one("span.thmb img") else ""
    except Exception as e:
        print(f"[Erreur] Impossible de récupérer l'image de couverture: {e}")
    
    try:
        webtoon_info['genre'] = soup.select_one("h2.genre").text.strip() if soup.select_one("h2.genre") else ""
    except Exception as e:
        print(f"[Erreur] Impossible de récupérer le genre: {e}")

    try:
        authors_info = []
        author_sections = soup.select("div.ly_creator_in")
        
        for section in author_sections:
            name = section.select_one("h3.title").text.strip() if section.select_one("h3.title") else ""
            try:
                desc = section.select_one("p.desc").text.strip() if section.select_one("p.desc") else ""
            except Exception as e:
                print(f"[Erreur] Impossible de récupérer la description de auteurs {name}: {e}")
            
            authors_info.append({"name": name, "description": desc})
        
        webtoon_info['authors'] = authors_info

    except Exception as e:
        print(f"[Erreur] Impossible de récupérer les informations des auteurs: {e}")


    try:
        webtoon_info['views'] = soup.select_one("ul.grade_area li span.ico_view + em").text.strip() if soup.select_one("ul.grade_area li span.ico_view + em") else ""
    except Exception as e:
        print(f"[Erreur] Impossible de récupérer le nombre de vues: {e}")

    try:
        webtoon_info['subscribers'] = soup.select_one("ul.grade_area li span.ico_subscribe + em").text.strip() if soup.select_one("ul.grade_area li span.ico_subscribe + em") else ""
    except Exception as e:
        print(f"[Erreur] Impossible de récupérer le nombre d'abonnés: {e}")

    try:
        webtoon_info['rating'] = soup.select_one("ul.grade_area li span.ico_grade5 + em").text.strip() if soup.select_one("ul.grade_area li span.ico_grade5 + em") else ""
    except Exception as e:
        print(f"[Erreur] Impossible de récupérer la note: {e}")

    try:
        webtoon_info['summary'] = soup.select_one("p.summary").text.strip() if soup.select_one("p.summary") else ""
    except Exception as e:
        print(f"[Erreur] Impossible de récupérer le résumé: {e}")

    try:
        webtoon_info['day_info'] = soup.select_one("p.day_info").text.strip() if soup.select_one("p.day_info") else ""
    except Exception as e:
        print(f"[Erreur] Impossible de récupérer le jour de publication: {e}")

    # Récupérer le QR code
    try:
        qr_code_element = soup.select_one("div.detail_install_app img.img_qrcode")
        qr_code_src = qr_code_element['src'] if qr_code_element else ""
        webtoon_info['qr_code'] = f"https://www.webtoons.com{qr_code_src}" if qr_code_src.startswith("/") else qr_code_src
    except Exception as e:
        print(f"[Erreur] Impossible de récupérer le QR code: {e}")
    
    # Récupérer les épisodes`
    try:
        webtoon_info['episodes'] = await get_webtoon_episodes_async(webtoon_url)
    except Exception as e:
        print(f"[Erreur] Impossible de récupérer les épisodes: {e}")
        webtoon_info['episodes'] = []

    return webtoon_info

# Fonction pour traiter chaque webtoon de manière asynchrone
async def process_webtoon(genre_url, webtoon_url, processed_webtoons, day_filter):
    # Récupère le webtoon existant dans MongoDB pour vérifier la date de dernière mise à jour
    webtoon_record = collection.find_one({"url": webtoon_url}) or {}
    last_update = webtoon_record.get("last_update", None)
    day_info = webtoon_record.get("day_info", "")
    
    # Vérifier si l'URL est déjà traitée
    if is_url_processed(webtoon_url, last_update):
        print(f"[INFO] Webtoon '{webtoon_url}' déjà traité, passage au suivant.")
        return None

    # Vérifier si le webtoon doit être mis à jour en fonction de `day_info`
    if should_update_webtoon(day_info, day_filter, last_update):
        webtoon_details = await get_webtoon_details_async(webtoon_url)
        if webtoon_details:
            webtoon_details["url"] = webtoon_url
            processed_webtoons.add(webtoon_url)
            print(f"[INFO] Détails du webtoon traités pour '{webtoon_url}'")
            return webtoon_details
    return None

async def get_webtoons_in_genre_async(genre_url, semaphore, webtoon_limit=None, batch_size=2, day_filter=None):
    async with semaphore:
        processed_webtoons = set()
        response = await fetch_with_retry_async(genre_url)
        if response is None:
            return processed_webtoons

        soup = BeautifulSoup(response.text, 'html.parser')
        webtoon_cards = soup.select("ul.card_lst li a")

        tasks = []
        webtoon_details_list = []
        
        for index, card in enumerate(webtoon_cards):
            if webtoon_limit is not None and index >= webtoon_limit:
                break

            webtoon_url = card["href"]
            # Créer une tâche de traitement de webtoon et l'ajouter à la liste des tâches
            tasks.append(process_webtoon(genre_url, webtoon_url, processed_webtoons, day_filter))

            # Lorsque `tasks` atteint la taille de `batch_size`, traiter le lot
            if len(tasks) >= batch_size:
                # Exécuter les tâches en parallèle et ajouter les résultats à la liste des détails
                webtoon_details_list += await asyncio.gather(*tasks)
                # Filtrer les `None`
                webtoon_details_list = [details for details in webtoon_details_list if details]
                # Insérer le lot de détails dans MongoDB avec `batch_upsert`
                batch_upsert(webtoon_details_list)
                # Réinitialiser `tasks` et `webtoon_details_list`
                tasks.clear()
                webtoon_details_list.clear()

        # Traiter les tâches restantes
        if tasks:
            webtoon_details_list += await asyncio.gather(*tasks)
            webtoon_details_list = [details for details in webtoon_details_list if details]
            batch_upsert(webtoon_details_list)
            
        return processed_webtoons



# Fonction asynchrone pour transférer les données mises à jour vers HDFS
async def transfer_updated_data_to_hdfs(batch_size):
    if not USE_HDFS:
        print("[INFO] Le transfert vers HDFS est désactivé.")
        return

    hdfs_file_path = f"{HDFS_DIR}/webtoon_data.json"
    temp_hdfs_file_path = f"{HDFS_DIR}/webtoon_data_temp.json"

    try:
        latest_updates = {}
        total_docs = collection.count_documents({})
        for start in range(0, total_docs, batch_size):
            docs_batch = list(collection.find().skip(start).limit(batch_size))
            for doc in docs_batch:
                doc["_id"] = str(doc["_id"])
                url = doc["url"]
                last_update = doc.get("last_update")
                if url not in latest_updates or last_update > latest_updates[url]["last_update"]:
                    latest_updates[url] = doc

        with hdfs_client.write(temp_hdfs_file_path, encoding='utf-8', overwrite=True) as writer:
            if hdfs_client.status(hdfs_file_path, strict=False):
                with hdfs_client.read(hdfs_file_path, encoding='utf-8') as reader:
                    for line in reader:
                        existing_doc = json.loads(line.strip())
                        url = existing_doc["url"]
                        if url in latest_updates:
                            writer.write(json.dumps(latest_updates[url]) + "\n")
                            latest_updates.pop(url)
                        else:
                            writer.write(json.dumps(existing_doc) + "\n")

            for new_doc in latest_updates.values():
                writer.write(json.dumps(new_doc) + "\n")

        hdfs_client.delete(hdfs_file_path)
        hdfs_client.rename(temp_hdfs_file_path, hdfs_file_path)
        print(f"[INFO] Transfert vers HDFS terminé. Données mises à jour dans {hdfs_file_path}.")
    except Exception as e:
        print(f"[ERREUR] Erreur lors du transfert des données vers HDFS : {e}")

# Fonction principale asynchrone avec limite d'instances
async def extract_and_store_webtoons_async(genres_url, webtoon_limit=None, batch_size=20, day_filter=None, instance_limit=5):
    semaphore = asyncio.Semaphore(instance_limit)
    response = await fetch_with_retry_async(genres_url)
    if response is None:
        print("[ERREUR] Impossible de récupérer la liste des genres.")
        return
    
    soup = BeautifulSoup(response.text, 'html.parser')
    genres = soup.select("ul.snb._genre li a")
    tasks = []
    for genre in genres:
        genre_url = genre["href"]
        genre_name = genre.text.strip().lower()
        print(f"[INFO] Extraction des webtoons dans le genre : {genre_name.capitalize()}")
        
        tasks.append(get_webtoons_in_genre_async(genre_url, semaphore, webtoon_limit=webtoon_limit, batch_size=batch_size, day_filter=day_filter))

    await asyncio.gather(*tasks)
    if USE_HDFS:
        await transfer_updated_data_to_hdfs(batch_size)

# Exécuter le programme asynchrone avec une limite d'instances
if __name__ == "__main__":
    asyncio.run(extract_and_store_webtoons_async(
        genres_url="https://www.webtoons.com/fr/genres",
        # webtoon_limit=5,
        batch_size=20,
        instance_limit=20,  # Nombre maximum de tâches simultanées
        # day_filter="LUNDI"
    ))