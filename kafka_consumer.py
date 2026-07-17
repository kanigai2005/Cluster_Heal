import time
import json
import random
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
        
        if KAFKA_AVAILABLE:
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
                        # Format: {"timestamp": ..., "value": ..., "metric": ..., "labels": {...}}
                        if isinstance(payload, dict) and "metric" in payload and "labels" in payload:
                            normalized["type"] = "prometheus_adapter_metric"
                            normalized["metric_name"] = payload.get("metric", "")
                            normalized["metric_value"] = payload.get("value", 0.0)
                            normalized["labels"] = payload.get("labels", {})
                            
                            # Safely extract pod identity and namespace from Labels
                            labels = payload.get("labels", {})
                            normalized["pod_name"] = labels.get("pod", labels.get("container", "unknown-pod"))
                            normalized["namespace"] = labels.get("namespace", "default")
                        
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
                
        # If no real messages were polled (not connected or silent topic),
        # automatically generate simulated real-time telemetry events to keep charts alive
        if not polled_data:
            import streamlit as st
            active_pods = st.session_state.get('pods', [])
            print("==================================DUMMY POLLED DATA================================\n")
            timestamp_ms = int(time.time() * 1000)
            timestamp_str = time.strftime("%H:%M:%S")
            
            # Generate highly detailed telemetry signals matching our actual active Kubernetes pods
            if active_pods:
                for pod in random.sample(active_pods, min(len(active_pods), random.randint(1, 3))):
                    normalized_msg = {
                        "source": "SIMULATION",
                        "topic": topic,
                        "partition": 0,
                        "offset": random.randint(10000, 50000),
                        "timestamp": timestamp_ms,
                        "timestamp_str": timestamp_str,
                        "type": "sre_platform_telemetry",
                        "pod_name": pod["name"],
                        "namespace": pod["namespace"],
                        "cpu_pct": pod["cpu_pct"],
                        "memory_mb": pod["memory_mb"],
                        "status": pod["status"],
                        "raw_payload": {
                            "metric": "container_cpu_usage_seconds_total",
                            "value": round(pod["cpu"], 2),
                            "labels": {
                                "pod": pod["name"],
                                "namespace": pod["namespace"]
                            }
                        }
                    }
                    polled_data.append(normalized_msg)
            else:
                for _ in range(random.randint(1, 3)):
                    normalized_msg = {
                        "source": "SIMULATION",
                        "topic": topic,
                        "partition": 0,
                        "offset": random.randint(10000, 50000),
                        "timestamp": timestamp_ms,
                        "timestamp_str": timestamp_str,
                        "type": "sre_platform_telemetry",
                        "pod_name": "esp-traffic-camera-abcde-fgh",
                        "namespace": "town-traffic",
                        "cpu_pct": round(random.uniform(10.0, 45.0), 1),
                        "memory_mb": round(random.uniform(150.0, 320.0), 1),
                        "status": "HEALTHY",
                        "raw_payload": {
                            "metric": "container_cpu_usage_seconds_total",
                            "value": round(random.uniform(1.2, 3.8), 2),
                            "labels": {
                                "pod": "esp-traffic-camera-abcde-fgh",
                                "namespace": "town-traffic"
                            }
                        }
                    }
                    polled_data.append(normalized_msg)
                
        return polled_data
