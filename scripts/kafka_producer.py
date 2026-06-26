"""
Kafka Producer — Dataset NASA HTTP Access Logs (Kaggle)
Lit le fichier log ligne par ligne et publie chaque ligne
dans le topic Kafka 'web-logs', simulant un flux en temps réel.

Exemple de ligne NASA :
in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839
"""

import time
from kafka import KafkaProducer

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"             # Port exposé par Docker vers Windows
TOPIC        = "web-logs"                   # Topic Kafka cible
LOG_FILE     = "data/NASA_access_log_Aug95.log"  # Chemin relatif depuis tp-bigdata/
DELAY        = 0.01    # Délai entre chaque message en secondes (0.01 = ~100 msg/sec)
MAX_LINES    = 1000000 # Nombre max de lignes à envoyer (None = tout le fichier)

# ── Producer ──────────────────────────────────────────────────────────────────
def main():
    # Créer le producer Kafka
    # acks="all"   : attendre la confirmation de tous les brokers avant de continuer
    # retries=5    : réessayer 5 fois en cas d'échec d'envoi
    # retry_backoff_ms=500 : attendre 500ms entre chaque tentative
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: v.encode("utf-8"),  # Encodage string → bytes
        acks="all",
        retries=5,
        retry_backoff_ms=500
    )

    print(f"[Producer] Connexion à Kafka sur {KAFKA_BROKER}")
    print(f"[Producer] Publication dans le topic '{TOPIC}'")
    print(f"[Producer] Fichier source : {LOG_FILE}")
    print("-" * 60)

    sent    = 0   # Compteur de messages envoyés
    skipped = 0   # Compteur de lignes vides ignorées

    # Lire le fichier ligne par ligne (errors="ignore" gère les caractères non-UTF8)
    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()  # Supprimer les espaces et sauts de ligne

            # Ignorer les lignes vides
            if not line:
                skipped += 1
                continue

            # Publier la ligne dans le topic Kafka
            producer.send(TOPIC, value=line)
            sent += 1

            # Afficher la progression tous les 1000 messages
            if sent % 1000 == 0:
                print(f"[Producer] {sent:,} messages envoyés...")

            # Arrêter si la limite de lignes est atteinte
            if MAX_LINES and sent >= MAX_LINES:
                print(f"[Producer] Limite de {MAX_LINES:,} lignes atteinte.")
                break

            # Pause pour simuler un flux en temps réel
            time.sleep(DELAY)

    # S'assurer que tous les messages en attente sont bien envoyés avant de fermer
    producer.flush()
    producer.close()

    print("-" * 60)
    print(f"[Producer] Terminé. {sent:,} messages envoyés, {skipped} lignes vides ignorées.")

if __name__ == "__main__":
    main()
