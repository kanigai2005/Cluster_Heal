import time
import json
import random
from typing import List, Dict

class TelemetryConsumerSimulator:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.servers = bootstrap_servers
        self.message_buffer = []

    def poll_messages(self, topic: str = "sre-pod-telemetry", limit: int = 5) -> List[Dict]:
        """
        Polls from the Kafka topic buffer and returns structured payloads.
        In a real Kafka client, this would execute: consumer.poll(timeout=1.0)
        """
        polled_data = []
        # Simulate active partition consumer offset reads
        for _ in range(limit):
            # Simulated high-frequency message stream content
            msg = {
                "topic": topic,
                "partition": 0,
                "offset": random.randint(10000, 50000),
                "timestamp": int(time.time() * 1000)
            }
            polled_data.append(msg)
        return polled_data
