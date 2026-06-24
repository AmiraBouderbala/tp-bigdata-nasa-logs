# Real-Time Web Log Analysis Pipeline

A Big Data pipeline for real-time analysis of NASA HTTP access logs using Apache Kafka, Spark Structured Streaming, Elasticsearch, and Kibana — all containerized with Docker Compose.

## Technologies
- **Apache Kafka** — Log ingestion and streaming
- **Apache Spark Structured Streaming** — Real-time data processing
- **Elasticsearch** — Indexing and storage of results
- **Kibana** — Interactive dashboard visualization
- **Docker Compose** — Service orchestration

## Dataset
NASA HTTP Access Logs (August 1995) — available on Kaggle :
https://www.kaggle.com/datasets/adchatakora/nasa-http-access-logs

The dataset contains ~1.5 million real HTTP requests recorded on NASA's Kennedy Space Center web servers.

## Quick Start

### Prerequisites
- Docker Desktop with WSL2 enabled (Windows)
- Python 3.12+
- NASA dataset placed at `data/NASA_access_log_Aug95.log`

### Launch

```bash
# 1. Start all services
docker compose up -d

# 2. Wait 60 seconds, then create the Kafka topic
docker exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --topic web-logs --partitions 1 --replication-factor 1

# 3. Restart Spark so it finds the topic
docker compose restart spark

# 4. Install dependencies and run the Kafka Producer
pip install kafka-python-ng==2.2.3
python scripts/kafka_producer.py

# 5. Open Kibana dashboard
# http://localhost:5601

# 6. Open Elasticsearch
# http://localhost:9200
```

### Shutdown

```bash
docker compose down
```

## Pipeline Architecture
[NASA Log File] → [Kafka Producer (Python)] → [Kafka Topic: web-logs]

→ [Spark Structured Streaming] → [4 Elasticsearch Indices] → [Kibana Dashboard]

## Elasticsearch Indices

| Index | Content |
|-------|---------|
| `nasa-logs-raw` | All parsed log entries |
| `nasa-logs-errors` | HTTP 404 and 500 errors only |
| `nasa-logs-traffic` | Request count aggregated per minute |
| `nasa-logs-top-hosts` | Most active hosts per minute |

## Project Structure
tp-bigdata/

├── docker-compose.yml         # Service orchestration

├── data/                      # NASA dataset (not included — download from Kaggle)

├── scripts/

│   └── kafka_producer.py      # Reads log file and publishes to Kafka

└── spark/

├── Dockerfile             # Custom Spark image with pre-downloaded JARs

└── streaming_job.py       # Spark Structured Streaming job

## Services & Ports

| Service | Port | URL |
|---------|------|-----|
| Kafka | 9092 | localhost:9092 |
| Elasticsearch | 9200 | http://localhost:9200 |
| Kibana | 5601 | http://localhost:5601 |

## Notes
- The NASA dataset file (168 MB) is excluded from this repository. Download it from Kaggle and place it in the `data/` folder.
- The Kafka topic must be created manually before starting Spark.
