import time
import json
import random
from typing import Dict, List

# Try to import real KafkaProducer
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Simulating Prometheus scrape to Kafka producer pipeline
class TelemetryProducerSimulator:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.servers = bootstrap_servers
        self.connected = False
        self.producer = None
        
        if KAFKA_AVAILABLE:
            try:
                # Use a fast 1-second timeout to prevent blocking Streamlit if local broker is down
                self.producer = KafkaProducer(
                    bootstrap_servers=self.servers.split(","),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    request_timeout_ms=1000,
                    max_block_ms=1000,
                    retries=0
                )
                self.connected = True
                print(f"✅ Real Kafka Producer successfully initialized on {self.servers}")
            except Exception as e:
                print(f"⚠️ Real Kafka Producer failed to initialize ({e}). Operating in simulation mode.")

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
            timestamp = time.strftime("%H:%M:%S")
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
            
            if self.connected and self.producer:
                try:
                    # Publish message with pod name as partition key to ensure order
                    future = self.producer.send(topic, key=pod["name"].encode('utf-8'), value=payload)
                    log_entry = f"[{timestamp}] 📡 REAL KAFKA PUSH: key={pod['name']} to topic={topic}"
                except Exception as e:
                    log_entry = f"[{timestamp}] ⚠️ REAL KAFKA PUSH FAILED ({e}): key={pod['name']} (Simulated fallback)"
            else:
                log_entry = f"[{timestamp}] 💻 SIM PUSH: key={pod['name']} to topic={topic} partitions=1 offset={random.randint(1000, 9999)}"
            
            logs.append(log_entry)
            
        if self.connected and self.producer:
            try:
                self.producer.flush()
            except Exception:
                pass
                
        return logs

    def send_event(self, event_type: str, message: str, topic: str = "sre-pod-events") -> str:
        """Publishes SRE remediation events and alerts to Kafka for downstream consumption"""
        timestamp = time.strftime("%H:%M:%S")
        payload = {
            "event_type": event_type,
            "message": message,
            "timestamp": timestamp,
            "timestamp_ms": int(time.time() * 1000)
        }
        if self.connected and self.producer:
            try:
                self.producer.send(topic, value=payload)
                self.producer.flush()
                return f"[{timestamp}] Real event sent to {topic}"
            except Exception as e:
                return f"[{timestamp}] Real event publish failed ({e})"
        return f"[{timestamp}] Simulated event sent to {topic}"
