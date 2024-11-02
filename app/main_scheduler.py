from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, timedelta
import asyncio
from script_scraping import extract_and_store_webtoons_async
from script_scraping_comment import fetch_comments_for_all_episodes, transfer_updated_comments_to_hdfs

# Initialisation du planificateur APScheduler
scheduler = BlockingScheduler()

def run_comment_update():
    print(f"[INFO] Début de la mise à jour des commentaires à {datetime.now()}")
    fetch_comments_for_all_episodes(batch_size=50, comment_limit=5, reply_limit=5)
    transfer_updated_comments_to_hdfs(batch_size=20)
    print(f"[INFO] Fin de la mise à jour des commentaires et transfert vers HDFS à {datetime.now()}")

def run_extraction():
    print(f"[INFO] Début de l'extraction des webtoons à {datetime.now()}")
    # Appeler la fonction asynchrone en utilisant asyncio.run()
    asyncio.run(extract_and_store_webtoons_async(
        genres_url="https://www.webtoons.com/fr/genres",
        # webtoon_limit=2,
        batch_size=20,
        instance_limit=20  # Nombre maximum de tâches simultanées
        # day_filter="LUNDI"
    ))
    print(f"[INFO] Fin de l'extraction des webtoons à {datetime.now()}")
    
    # Appeler l'update des commentaires après extraction
    run_comment_update()

start_time_extraction = datetime.now() + timedelta(seconds=10)

# Planifier run_extraction pour s'exécuter toutes les 24 heures
scheduler.add_job(run_extraction, 'interval', hours=24, start_date=start_time_extraction)

# Lancer le planificateur
try:
    print("[INFO] Lancement du planificateur APScheduler")
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    print("[INFO] Arrêt du planificateur")
