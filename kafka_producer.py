import time
import json
import random
from typing import Dict, List

# Simulating Prometheus scrape to Kafka producer pipeline
class TelemetryProducerSimulator:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.servers = bootstrap_servers
        self.connected = False
        # In a real environment, we would initialize confluent_kafka.Producer or kafka.KafkaProducer here
        # e.g.:
        # try:
        #     from kafka import KafkaProducer
        #     self.producer = KafkaProducer(bootstrap_servers=self.servers)
        #     self.connected = True
        # except Exception:
        #     self.producer = None

    def serialize_telemetry(self, pod: Dict) -> str:
        payload = {
            "name": pod["name"],
            "namespace": pod["namespace"],
            "deployment": pod["deployment"],
            "cpu_pct": pod["cpu_pct"],
            "memory_mb": pod["memory_mb"],
            "memory_limit": pod["memory_limit"],
            "restarts": pod["restarts"],
            "status": pod["status"],
            "replicas": pod["replicas"],
            "anomaly_score": pod.get("anomalyScore", 0.9),
            "is_anomaly": pod.get("isAnomaly", False),
            "timestamp_ms": int(time.time() * 1000)
        }
        return json.dumps(payload)

    def send_telemetry_batch(self, pods: List[Dict], topic: str = "sre-pod-telemetry") -> List[str]:
        logs = []
        for pod in pods:
            serialized = self.serialize_telemetry(pod)
            # Simulating push to broker
            timestamp = time.strftime("%H:%M:%S")
            log_entry = f"[{timestamp}] PUSHED key={pod['name']} to topic={topic} partitions=1 offset={random.randint(1000, 9999)}"
            logs.append(log_entry)
        return logs
