"""
Kafka Producer — Dataset NASA HTTP Access Logs (Kaggle)
Lit le fichier log ligne par ligne et publie dans le topic 'web-logs'.

Exemple de ligne NASA :
in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839
"""

import time
from kafka import KafkaProducer

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"    # accessible depuis Windows via le port exposé
TOPIC        = "web-logs"
LOG_FILE     = "data/NASA_access_log_Aug95.log"   # chemin relatif depuis tp-bigdata/
DELAY        = 0.01    # secondes entre chaque message (0.01 = ~100 msg/sec)
MAX_LINES    = 1000000   # mettre None pour envoyer tout le fichier

# ── Producer ──────────────────────────────────────────────────────────────────
def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=500
    )

    print(f"[Producer] Connexion à Kafka sur {KAFKA_BROKER}")
    print(f"[Producer] Publication dans le topic '{TOPIC}'")
    print(f"[Producer] Fichier source : {LOG_FILE}")
    print("-" * 60)

    sent  = 0
    skipped = 0

    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                skipped += 1
                continue

            producer.send(TOPIC, value=line)
            sent += 1

            if sent % 1000 == 0:
                print(f"[Producer] {sent:,} messages envoyés...")

            if MAX_LINES and sent >= MAX_LINES:
                print(f"[Producer] Limite de {MAX_LINES:,} lignes atteinte.")
                break

            time.sleep(DELAY)

    producer.flush()
    producer.close()
    print("-" * 60)
    print(f"[Producer] Terminé. {sent:,} messages envoyés, {skipped} lignes vides ignorées.")

if __name__ == "__main__":
    main()