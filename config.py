import os

# Site Reliability Engineering Configuration Parameters

# Gemini / Groq API Configuration
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# Kafka Broking Server Setup
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TELEMETRY = "sre-pod-telemetry"
KAFKA_TOPIC_ANOMALIES = "sre-pod-anomalies"

# Prometheus Scraper Configurations
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
SCRAPE_INTERVAL_SECONDS = 3.0

# Q-Learning Hyperparameters
RL_LEARNING_RATE = 0.2
RL_DISCOUNT_FACTOR = 0.8
RL_INITIAL_EPSILON = 0.3
RL_DECAY_RATE = 0.01

# SRE Thresholds
CPU_WARNING_THRESHOLD_PCT = 70.0
CPU_CRITICAL_THRESHOLD_PCT = 90.0
MEM_WARNING_THRESHOLD_PCT = 70.0
MEM_CRITICAL_THRESHOLD_PCT = 90.0
ANOMALY_SCORE_THRESHOLD = -0.5
