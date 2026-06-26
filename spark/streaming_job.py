"""
Spark Structured Streaming — Analyse de Logs Web NASA en Temps Réel
Pipeline : Kafka (web-logs) → Spark → Elasticsearch (4 index)
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, window, count, to_timestamp
)

# Regex pour parser le format Apache Combined Log
# Groupes : host | timestamp | method | url | status_code | bytes
LOG_REGEX = (
    r'^(\S+)'                        # Groupe 1 : host
    r'\s+\S+\s+\S+\s+'              # Ignorer "- -"
    r'\[([^\]]+)\]'                  # Groupe 2 : timestamp entre crochets
    r'\s+"(\S+)\s+(\S+)\s+[^"]*"'  # Groupe 3 : method | Groupe 4 : url
    r'\s+(\d{3})'                    # Groupe 5 : code HTTP
    r'\s+(\S+)'                      # Groupe 6 : taille en octets
)

# Adresse Elasticsearch (réseau interne Docker)
ES_HOST = "http://elasticsearch:9200"


def main():

    # Initialisation de la session Spark
    spark = SparkSession.builder \
        .appName("NASA-WebLog-Streaming") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Lecture du flux depuis Kafka
    # startingOffsets="earliest" : relire depuis le début du topic au redémarrage
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "web-logs") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Convertir les messages Kafka (binaire) en chaînes UTF-8
    raw_logs = raw_stream.selectExpr("CAST(value AS STRING) as raw_log")

    # Extraire les champs de chaque ligne avec la regex
    # Les lignes ne correspondant pas sont filtrées (host == "")
    parsed = raw_logs.select(
        regexp_extract("raw_log", LOG_REGEX, 1).alias("host"),
        regexp_extract("raw_log", LOG_REGEX, 2).alias("timestamp_str"),
        regexp_extract("raw_log", LOG_REGEX, 3).alias("method"),
        regexp_extract("raw_log", LOG_REGEX, 4).alias("url"),
        regexp_extract("raw_log", LOG_REGEX, 5).alias("status_code"),
        regexp_extract("raw_log", LOG_REGEX, 6).alias("bytes"),
    ).filter(col("host") != "")

    # Convertir le timestamp NASA en type Timestamp Spark
    # Format : "01/Aug/1995:00:00:01 -0400"
    parsed = parsed.withColumn(
        "event_time",
        to_timestamp(col("timestamp_str"), "dd/MMM/yyyy:HH:mm:ss Z")
    )

    # Filtrer uniquement les erreurs 404 et 500
    errors = parsed.filter(col("status_code").isin("404", "500"))

    # Compter les requêtes par fenêtre d'1 minute
    # withWatermark : tolérance de 5 min pour les données en retard
    traffic = parsed \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(window(col("event_time"), "1 minute")) \
        .agg(count("*").alias("request_count"))

    # Compter les requêtes par host par fenêtre d'1 minute
    top_hosts = parsed \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(window(col("event_time"), "1 minute"), col("host")) \
        .agg(count("*").alias("hit_count"))

    # Envoi générique d'un micro-batch vers un index Elasticsearch via l'API bulk
    def send_to_es(df, epoch_id, index_name):
        from elasticsearch import Elasticsearch, helpers
        records = df.toJSON().collect()
        if not records:
            return
        es = Elasticsearch(ES_HOST)
        actions = [{"_index": index_name, "_source": json.loads(r)} for r in records]
        helpers.bulk(es, actions)
        print(f"[ES] {len(actions)} docs → '{index_name}' (batch {epoch_id})")

    # Aplatir la colonne window (struct) en deux strings avant l'envoi
    def send_windowed(df, epoch_id, index_name):
        df2 = df.withColumn("window_start", col("window.start").cast("string")) \
                .withColumn("window_end",   col("window.end").cast("string")) \
                .drop("window")
        send_to_es(df2, epoch_id, index_name)

    # Convertir event_time en string pour éviter le filtre temporel de Kibana
    # (les données datent de 1995, Kibana filtre par défaut sur l'année courante)
    def send_errors(df, epoch_id):
        from elasticsearch import Elasticsearch, helpers
        df2 = df.withColumn("event_time_str", col("event_time").cast("string")) \
                .drop("event_time")
        records = df2.toJSON().collect()
        if not records:
            return
        es = Elasticsearch(ES_HOST)
        actions = [{"_index": "nasa-logs-errors", "_source": json.loads(r)} for r in records]
        helpers.bulk(es, actions)
        print(f"[ES] {len(actions)} docs → 'nasa-logs-errors' (batch {epoch_id})")

    # Requête 1 — Tous les logs parsés → nasa-logs-raw
    q1 = parsed.writeStream \
        .foreachBatch(lambda df, eid: send_to_es(df, eid, "nasa-logs-raw")) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/ck/raw") \
        .trigger(processingTime="10 seconds") \
        .start()

    # Requête 2 — Erreurs HTTP 404 et 500 → nasa-logs-errors
    q2 = errors.writeStream \
        .foreachBatch(send_errors) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/ck/errors") \
        .trigger(processingTime="10 seconds") \
        .start()

    # Requête 3 — Trafic agrégé par minute → nasa-logs-traffic
    q3 = traffic.writeStream \
        .foreachBatch(lambda df, eid: send_windowed(df, eid, "nasa-logs-traffic")) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/ck/traffic") \
        .trigger(processingTime="10 seconds") \
        .start()

    # Requête 4 — Hosts les plus actifs par minute → nasa-logs-top-hosts
    q4 = top_hosts.writeStream \
        .foreachBatch(lambda df, eid: send_windowed(df, eid, "nasa-logs-top-hosts")) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/ck/top-hosts") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("=" * 60)
    print("Spark Streaming actif — en attente de données Kafka...")
    print("=" * 60)

    # Bloquer jusqu'à l'arrêt d'une des 4 requêtes (erreur ou arrêt manuel)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
