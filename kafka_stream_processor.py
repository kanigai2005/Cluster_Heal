# kafka_stream_processor.py - Consumes Kafka stream and processes
import json
import threading
from typing import Dict, List, Callable
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

from config import config
from anomaly_detection import anomaly_detector

class KafkaStreamProcessor:
    def __init__(self):
        self.consumer = None
        self.callbacks = []
        self.latest_metrics: Dict = {}
        self.running = True
        self._connect()
    
    def _connect(self):
        if not config.kafka_enabled:
            print("⚠️ Kafka disabled")
            return
        
        try:
            conf = {
                'bootstrap.servers': config.kafka_bootstrap_servers,
                'group.id': config.kafka_consumer_group,
                'auto.offset.reset': 'latest',
                'enable.auto.commit': True,
            }
            self.consumer = Consumer(conf)
            self.consumer.subscribe([config.kafka_topic_metrics])
            print(f"✅ Kafka consumer subscribed to {config.kafka_topic_metrics}")
        except Exception as e:
            print(f"⚠️ Kafka consumer error: {e}")
            self.consumer = None
    
    def register_callback(self, callback: Callable):
        self.callbacks.append(callback)
    
    def process_message(self, message):
        """Process a single Kafka message"""
        try:
            event = json.loads(message.value().decode('utf-8'))
            pod_key = event.get('pod_key')
            
            if not pod_key:
                return
            
            # Store latest metrics
            self.latest_metrics[pod_key] = {
                'cpu': event.get('cpu', 0),
                'memory_mb': event.get('memory_mb', 0),
                'io_kbps': event.get('io_kbps', 0),
                'namespace': event.get('namespace', ''),
                'pod_name': event.get('pod_name', ''),
                'node': event.get('node', ''),
                'timestamp': datetime.fromtimestamp(event.get('timestamp', time.time()))
            }
            
            # Run anomaly detection
            anomaly_detector.add_metric(pod_key, event.get('cpu', 0), event.get('memory_mb', 0))
            is_anomaly, score = anomaly_detector.detect(pod_key, event.get('cpu', 0), event.get('memory_mb', 0))
            
            # Notify callbacks
            for callback in self.callbacks:
                callback(event, is_anomaly, score)
                
        except Exception as e:
            print(f"Message processing error: {e}")
    
    def start_consuming(self):
        """Start consuming messages"""
        if not self.consumer:
            print("⚠️ No Kafka consumer - running in demo mode")
            self._demo_mode()
            return
        
        print("🎧 Listening for Kafka messages...")
        
        while self.running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
                continue
            
            self.process_message(msg)
    
    def _demo_mode(self):
        """Demo mode when Kafka is not available"""
        print("📡 Demo mode - generating simulated metrics")
        import random, time
        
        pods = [
            'town-traffic/esp-traffic-camera',
            'campus-water/esp-water-sensor', 
            'kitchen-traffic/esp-smart-kitchen'
        ]
        
        while self.running:
            for pod_key in pods:
                ns, name = pod_key.split('/')
                event = {
                    'pod_key': pod_key,
                    'namespace': ns,
                    'pod_name': name,
                    'cpu': random.uniform(0.2, 0.8),
                    'memory_mb': random.uniform(100, 600),
                    'io_kbps': random.uniform(100, 3000),
                    'timestamp': time.time()
                }
                
                # Random anomaly
                if random.random() < 0.1:
                    event['cpu'] = random.uniform(0.9, 1.5)
                    event['memory_mb'] = random.uniform(800, 1500)
                
                self.process_message(type('Msg', (), {'value': lambda: json.dumps(event).encode()})())
                
                for callback in self.callbacks:
                    callback(event, event['cpu'] > 0.9, 0)
            
            time.sleep(3)
    
    def get_latest_metrics(self) -> Dict:
        return self.latest_metrics
    
    def start(self):
        thread = threading.Thread(target=self.start_consuming, daemon=True)
        thread.start()
        return thread

stream_processor = KafkaStreamProcessor()
