from hdfs import InsecureClient

client = InsecureClient('http://127.0.0.1:9870', user='root')

# Vérifier si le fichier existe déjà
if not client.status('/dossier/fichier.txt', strict=False):
    with client.write('/dossier/fichier.txt', encoding='utf-8') as writer:
        writer.write('Bonjour, HDFS!')
else:
    print("Le fichier existe déjà dans HDFS.")

# Lecture du fichier depuis HDFS
with client.read('/dossier/fichier.txt', encoding='utf-8') as reader:
    content = reader.read()
    print("Contenu du fichier:", content)
