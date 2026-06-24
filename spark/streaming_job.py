"""
Spark Structured Streaming — Analyse de logs NASA en temps réel
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, window, count, to_timestamp
)

LOG_REGEX = (
    r'^(\S+)'
    r'\s+\S+\s+\S+\s+'
    r'\[([^\]]+)\]'
    r'\s+"(\S+)\s+(\S+)\s+[^"]*"'
    r'\s+(\d{3})'
    r'\s+(\S+)'
)

ES_HOST = "http://elasticsearch:9200"

def main():
    spark = SparkSession.builder \
        .appName("NASA-WebLog-Streaming") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "web-logs") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    raw_logs = raw_stream.selectExpr("CAST(value AS STRING) as raw_log")

    parsed = raw_logs.select(
        regexp_extract("raw_log", LOG_REGEX, 1).alias("host"),
        regexp_extract("raw_log", LOG_REGEX, 2).alias("timestamp_str"),
        regexp_extract("raw_log", LOG_REGEX, 3).alias("method"),
        regexp_extract("raw_log", LOG_REGEX, 4).alias("url"),
        regexp_extract("raw_log", LOG_REGEX, 5).alias("status_code"),
        regexp_extract("raw_log", LOG_REGEX, 6).alias("bytes"),
    ).filter(col("host") != "")

    parsed = parsed.withColumn(
        "event_time",
        to_timestamp(col("timestamp_str"), "dd/MMM/yyyy:HH:mm:ss Z")
    )

    errors = parsed.filter(col("status_code").isin("404", "500"))

    traffic = parsed \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(window(col("event_time"), "1 minute")) \
        .agg(count("*").alias("request_count"))

    top_hosts = parsed \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(window(col("event_time"), "1 minute"), col("host")) \
        .agg(count("*").alias("hit_count"))

    def send_to_es(df, epoch_id, index_name):
        from elasticsearch import Elasticsearch, helpers
        records = df.toJSON().collect()
        if not records:
            return
        es = Elasticsearch(ES_HOST)
        actions = [{"_index": index_name, "_source": json.loads(r)} for r in records]
        helpers.bulk(es, actions)
        print(f"[ES] {len(actions)} docs → '{index_name}' (batch {epoch_id})")

    def send_windowed(df, epoch_id, index_name):
        df2 = df.withColumn("window_start", col("window.start").cast("string")) \
                .withColumn("window_end",   col("window.end").cast("string")) \
                .drop("window")
        send_to_es(df2, epoch_id, index_name)

    # ── NOUVELLE FONCTION pour les erreurs ───────────────────────────────────
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

    q1 = parsed.writeStream \
        .foreachBatch(lambda df, eid: send_to_es(df, eid, "nasa-logs-raw")) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/ck/raw") \
        .trigger(processingTime="10 seconds") \
        .start()

    # ── q2 utilise maintenant send_errors ────────────────────────────────────
    q2 = errors.writeStream \
        .foreachBatch(send_errors) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/ck/errors") \
        .trigger(processingTime="10 seconds") \
        .start()

    q3 = traffic.writeStream \
        .foreachBatch(lambda df, eid: send_windowed(df, eid, "nasa-logs-traffic")) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/ck/traffic") \
        .trigger(processingTime="10 seconds") \
        .start()

    q4 = top_hosts.writeStream \
        .foreachBatch(lambda df, eid: send_windowed(df, eid, "nasa-logs-top-hosts")) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/ck/top-hosts") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("=" * 60)
    print("Spark Streaming actif — en attente de données Kafka...")
    print("=" * 60)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()