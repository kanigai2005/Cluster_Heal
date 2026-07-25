import time
import json
import random
import socket
from typing import List, Dict

# Try to import real KafkaConsumer
try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

class TelemetryConsumerSimulator:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.servers = bootstrap_servers
        self.connected = False
        self.consumer = None
        
        # Verify socket connection first to ensure Kafka is actually reachable and listening
        is_reachable = False
        try:
            for server in self.servers.split(","):
                parts = server.strip().split(":")
                if len(parts) == 2:
                    host, port = parts[0], int(parts[1])
                    with socket.create_connection((host, port), timeout=0.5) as s:
                        is_reachable = True
                        break
        except Exception:
            pass
        
        if KAFKA_AVAILABLE and is_reachable:
            try:
                # Initialize real Kafka consumer with a fast timeout
                # It listens from the latest messages to ensure live processing
                self.consumer = KafkaConsumer(
                    bootstrap_servers=self.servers.split(","),
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    request_timeout_ms=1000,
                    consumer_timeout_ms=100,  # Prevent blocking Streamlit event loop
                )
                self.connected = True
                print(f"✅ Real Kafka Consumer successfully initialized on {self.servers}")
            except Exception as e:
                print(f"⚠️ Real Kafka Consumer failed to initialize ({e}). Operating in simulation mode.")
        else:
            if not is_reachable:
                print(f"⚠️ Kafka Broker on {self.servers} is unreachable (Socket check failed). Operating in simulation mode.")
            else:
                print("⚠️ Kafka library not available. Operating in simulation mode.")

    def poll_messages(self, topic: str = "sre-pod-telemetry", limit: int = 15) -> List[Dict]:
        """
        Polls from the Kafka topic buffer and returns structured payloads.
        Supports both Prometheus Kafka Adapter metrics and SRE platform custom telemetry formats.
        """
        polled_data = []
        
        if self.connected and self.consumer:
            try:
                # Ensure subscription to target topic is active
                subscribed_topics = list(self.consumer.subscription() or [])
                if topic not in subscribed_topics:
                    self.consumer.subscribe([topic])
                
                # Fetch a small batch of records to keep UI interactive
                records = self.consumer.poll(timeout_ms=150, max_records=limit)
                for tp, msg_list in records.items():
                    for msg in msg_list:
                        payload = msg.value
                        
                        # Process / normalize the message structure
                        normalized = {
                            "source": "REAL_KAFKA",
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "timestamp": msg.timestamp,
                            "raw_payload": payload,
                            "timestamp_str": time.strftime("%H:%M:%S", time.localtime(msg.timestamp / 1000.0 if msg.timestamp else time.time()))
                        }
                        
                        # Case 1: Prometheus Kafka Adapter JSON Output schema
                        # Format 1: {"timestamp": ..., "value": ..., "metric": ..., "labels": {...}}
                        # Format 2 (Standard Kafka exporter): {"timestamp": ..., "value": ..., "name": ..., "labels": {...}}
                        if isinstance(payload, dict) and ("metric" in payload or "name" in payload) and "labels" in payload:
                            normalized["type"] = "prometheus_adapter_metric"
                            normalized["metric_name"] = payload.get("metric") or payload.get("name", "")
                            
                            # Parse value safely (could be float, int, or numeric string)
                            raw_val = payload.get("value", 0.0)
                            try:
                                normalized["metric_value"] = float(raw_val)
                            except (ValueError, TypeError):
                                normalized["metric_value"] = 0.0
                                
                            normalized["labels"] = payload.get("labels", {})
                            
                            # Safely extract pod identity from multiple Prometheus label key variants
                            labels = payload.get("labels", {})
                            pod_id = labels.get("pod") or labels.get("pod_name") or labels.get("kubernetes_pod_name")
                            
                            # Skip metrics without pod-level identity (node/infrastructure metrics)
                            if not pod_id or pod_id in ("POD", ""):
                                continue
                            
                            normalized["pod_name"] = pod_id
                            normalized["namespace"] = labels.get("namespace", labels.get("kubernetes_namespace", "default"))
                        
                        # Case 2: Custom SRE platform telemetry
                        # Format: {"name": ..., "namespace": ..., "cpu_pct": ..., "memory_mb": ...}
                        elif isinstance(payload, dict) and "name" in payload and "cpu_pct" in payload:
                            normalized["type"] = "sre_platform_telemetry"
                            normalized["pod_name"] = payload.get("name", "")
                            normalized["namespace"] = payload.get("namespace", "default")
                            normalized["cpu_pct"] = payload.get("cpu_pct", 0.0)
                            normalized["memory_mb"] = payload.get("memory_mb", 0.0)
                            normalized["status"] = payload.get("status", "HEALTHY")
                        
                        else:
                            normalized["type"] = "unknown"
                            normalized["pod_name"] = "generic-stream"
                            normalized["namespace"] = "default"
                            
                        polled_data.append(normalized)
            except Exception as e:
                print(f"⚠️ Error polling real-time Kafka metrics: {e}")
                
        return polled_data

