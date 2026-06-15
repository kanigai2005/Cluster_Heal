# config.py - Central configuration
import os
from dataclasses import dataclass

@dataclass
class Config:
    # Prometheus
    prometheus_url: str = "http://localhost:9090"
    prometheus_timeout: int = 5
    
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_metrics: str = "sre-metrics"
    kafka_topic_alerts: str = "sre-alerts"
    kafka_consumer_group: str = "sre-group"
    
    # LLM
    groq_api_key: str = os.environ.get("GROQ_API_KEY", "")
    groq_model: str = "llama-3.1-8b-instant"
    
    # Monitoring
    refresh_interval: int = 5
    anomaly_threshold: float = 0.7
    
    # Streaming
    kafka_enabled: bool = True  # Set False to disable Kafka
    metric_change_threshold: float = 0.05  # 5% change triggers update

config = Config()
