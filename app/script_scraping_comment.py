import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from hdfs import InsecureClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configuration MongoDB, HDFS et Selenium
# MONGO_URI = "mongodb://localhost:27017"
MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "webtoons"
MONGO_WEBTOON_DATA_COLLECTION = "webtoon_data"
MONGO_COMMENTS_COLLECTION = "webtoon_comments"
HDFS_URL = "http://namenode:9870"
HDFS_DIR = "/webtoons_data"
USE_HDFS = True #False

try:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    comments_collection = db[MONGO_COMMENTS_COLLECTION]
    webtoon_data_collection = db[MONGO_WEBTOON_DATA_COLLECTION]
    print("[INFO] Connexion réussie à MongoDB")
except Exception as e:
    print(f"[ERREUR] Connexion échouée à MongoDB : {e}")

# Connexion HDFS
if USE_HDFS:
    try:
        hdfs_client = InsecureClient(HDFS_URL)
        if not hdfs_client.status(HDFS_DIR, strict=False):
            hdfs_client.makedirs(HDFS_DIR)
        print("[INFO] Connexion réussie à HDFS")
    except Exception as e:
        print(f"[ERREUR] Impossible de se connecter à HDFS : {e}")

# Fonction pour initialiser le navigateur Selenium
def init_driver():
    options = Options()
    options.add_argument('--headless')  # Activer le mode sans tête si nécessaire
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-webgl')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument('--use-gl=swiftshader')
    options.add_argument('--enable-unsafe-swiftshader')
    
    for attempt in range(3):  # Essaye de se connecter 3 fois
        try:
            # driver = webdriver.Chrome(options=options)
            driver = webdriver.Remote(
                command_executor='http://selenium-chrome:4444/wd/hub',
                options=options
            )
            return driver
        except Exception as e:
            print(f"[ERREUR] Échec de la connexion à Selenium (tentative {attempt + 1}) : {e}")
            time.sleep(5)  # Pause de 5 secondes entre les tentatives
    return None

# Fonction pour accepter les cookies
def accept_cookies(driver):
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "cmpwrapper"))
        )
        driver.execute_script("""
            const cmpWrapper = document.querySelector('#cmpwrapper');
            const shadowRoot = cmpWrapper.shadowRoot;
            const consentButton = shadowRoot.querySelector('div.cmpboxbtns a.cmpboxbtnyes');
            if (consentButton) {
                consentButton.click();
                console.log('Consentement accepté via Shadow DOM.');
            } else {
                console.log('Bouton de consentement non trouvé dans le Shadow DOM.');
            }
        """)
        print("[INFO] Script exécuté pour le consentement dans le Shadow DOM.")
    except Exception as e:
        print("[INFO] Aucun bouton de consentement trouvé ou erreur lors du clic :", e)

# Récupérer les URLs des épisodes mis à jour aujourd'hui par lot
def get_episode_urls(batch_size):
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        total_episodes = webtoon_data_collection.count_documents({"last_update": {"$regex": today}})
        print(f"Total des épisodes mis à jour aujourd'hui dans webtoon_data : {total_episodes}")
        
        for start in range(0, total_episodes, batch_size):
            docs_batch = list(webtoon_data_collection.find({"last_update": {"$regex": today}})
                        .skip(start)
                        .limit(batch_size))
            
            urls = []
            for doc in docs_batch:
                for episode in doc.get("episodes", []):
                    episode_url = episode.get("url")
                    if episode_url:
                        urls.append(episode_url)
            
            print(f"[INFO] {len(urls)} URLs d'épisodes récupérées depuis webtoon_data (batch {start // batch_size + 1})")
            yield urls

    except Exception as e:
        print(f"[ERREUR] Erreur lors de la récupération des URLs depuis webtoon_data : {e}")

# Fonction pour récupérer les commentaires d'un épisode de manière synchrone
def fetch_episode_comments(episode_url, comment_limit=50, reply_limit=5):
    driver = init_driver()
    if not driver:
        print(f"[ERREUR] Impossible d'initialiser le driver pour {episode_url}")
        return []

    driver.get(episode_url)
    accept_cookies(driver)

    comments = []
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'li.wcc_CommentItem__root'))
        )
        
        comment_elements = driver.find_elements(By.CSS_SELECTOR, 'li.wcc_CommentItem__root')[:comment_limit]
        
        for i, comment_element in enumerate(comment_elements):
            username = comment_element.find_element(By.CSS_SELECTOR, 'span.wcc_CommentHeader__name').text.strip()
            date = comment_element.find_element(By.CSS_SELECTOR, 'time.wcc_CommentHeader__createdAt').text.strip()
            content = comment_element.find_element(By.CSS_SELECTOR, 'p.wcc_TextContent__content > span').text.strip()
            likes = int(comment_element.find_elements(By.CSS_SELECTOR, 'button.wcc_CommentReaction__action > span')[0].text.strip() or 0)
            dislikes = int(comment_element.find_elements(By.CSS_SELECTOR, 'button.wcc_CommentReaction__action > span')[1].text.strip() or 0)

            comment_data = {
                'username': username,
                'date': date,
                'content': content,
                'likes': likes,
                'dislikes': dislikes,
                'replies': []
            }

            if reply_limit > 0:
                try:
                    reply_elements = comment_element.find_elements(By.CSS_SELECTOR, 'li.wcc_CommentItem__replied')[:reply_limit]
                    for reply_element in reply_elements:
                        reply_username = reply_element.find_element(By.CSS_SELECTOR, 'span.wcc_CommentHeader__name').text.strip()
                        reply_date = reply_element.find_element(By.CSS_SELECTOR, 'time.wcc_CommentHeader__createdAt').text.strip()
                        reply_content = reply_element.find_element(By.CSS_SELECTOR, 'p.wcc_TextContent__content > span').text.strip()
                        comment_data['replies'].append({
                            'username': reply_username,
                            'date': reply_date,
                            'content': reply_content
                        })
                except Exception as e:
                    print(f"[INFO] Erreur lors de la récupération des réponses pour un commentaire : {e}")

            comments.append(comment_data)

        print(f"[INFO] Récupération terminée pour {episode_url}")
    
    except Exception as e:
        print(f"[ERREUR] Erreur lors de la récupération des commentaires pour {episode_url} : {e}")
    
    finally:
        driver.quit()
    
    return comments

# Fonction pour récupérer les commentaires pour tous les épisodes en parallèle
def fetch_comments_for_all_episodes(batch_size, comment_limit=50, reply_limit=5):
    with ThreadPoolExecutor() as executor:
        for episode_urls in get_episode_urls(batch_size):
            futures = {executor.submit(fetch_episode_comments, url, comment_limit, reply_limit): url for url in episode_urls}
            
            bulk_operations = []
            for future in as_completed(futures):
                episode_url = futures[future]
                try:
                    comments = future.result()
                    bulk_operations.append(UpdateOne(
                        {"episode_url": episode_url},
                        {"$set": {"comments": comments, "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
                        upsert=True
                    ))
                except Exception as e:
                    print(f"[ERREUR] Erreur lors de la récupération des commentaires pour {episode_url} : {e}")

            if bulk_operations:
                try:
                    comments_collection.bulk_write(bulk_operations, ordered=False)
                    print(f"[INFO] Batch de {len(bulk_operations)} opérations de mise à jour exécuté dans MongoDB.")
                except BulkWriteError as e:
                    print(f"[ERREUR] Erreur lors de la mise à jour en batch MongoDB : {e.details}")

# Fonction pour transférer les données vers HDFS
async def transfer_updated_comments_to_hdfs(batch_size):
    if not USE_HDFS:
        print("[INFO] Le transfert vers HDFS est désactivé.")
        return
    
    hdfs_file_path = f"{HDFS_DIR}/webtoon_comments.json"
    temp_hdfs_file_path = f"{HDFS_DIR}/webtoon_comments_temp.json"
    
    try:
        today = datetime.today().strftime("%Y-%m-%d")
        total_docs = comments_collection.count_documents({"last_updated": {"$regex": today}})
        
        latest_updates = {}
        for start in range(0, total_docs, batch_size):
            docs_batch = list(comments_collection.find({"last_updated": {"$regex": today}}).skip(start).limit(batch_size))
            for doc in docs_batch:
                doc["_id"] = str(doc["_id"])
                episode_url = doc["episode_url"]
                
                if episode_url not in latest_updates or doc["last_updated"] > latest_updates[episode_url]["last_updated"]:
                    latest_updates[episode_url] = doc
        
        # Ecrit les mises à jour dans un fichier temporaire pour gérer les doublons
        with hdfs_client.write(temp_hdfs_file_path, encoding='utf-8', overwrite=True) as writer:
            if hdfs_client.status(hdfs_file_path, strict=False):
                with hdfs_client.read(hdfs_file_path, encoding='utf-8') as reader:
                    for line in reader:
                        existing_doc = json.loads(line.strip())
                        episode_url = existing_doc["episode_url"]
                        
                        if episode_url in latest_updates:
                            writer.write(json.dumps(latest_updates[episode_url]) + "\n")
                            latest_updates.pop(episode_url)
                        else:
                            writer.write(json.dumps(existing_doc) + "\n")
            
            for new_doc in latest_updates.values():
                writer.write(json.dumps(new_doc) + "\n")
        
        hdfs_client.delete(hdfs_file_path)
        hdfs_client.rename(temp_hdfs_file_path, hdfs_file_path)

        print(f"[INFO] Transfert vers HDFS terminé. Données mises à jour dans {hdfs_file_path}.")

    except Exception as e:
        print(f"[ERREUR] Erreur lors du transfert des données vers HDFS : {e}")

# Appel principal
if __name__ == "__main__":
    fetch_comments_for_all_episodes(batch_size=1, comment_limit=5, reply_limit=5)


