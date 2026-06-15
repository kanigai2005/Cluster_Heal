# kafka_consumer.py - Kafka consumer for receiving metrics
import json
import threading
import time
from datetime import datetime
from typing import Dict, Callable
from confluent_kafka import Consumer, KafkaError, KafkaException
from config import config

class MetricsKafkaConsumer:
    """
    Consumes metrics from Kafka topic and makes them available for Streamlit.
    """
    
    def __init__(self):
        self.consumer = None
        self.latest_metrics: Dict = {}
        self.message_count = 0
        self.running = True
        self.callbacks = []
        self._connect()
    
    def _connect(self):
        """Connect to Kafka broker"""
        try:
            conf = {
                'bootstrap.servers': config.kafka_bootstrap_servers,
                'group.id': config.kafka_consumer_group,
                'auto.offset.reset': 'latest',
                'enable.auto.commit': True,
                'socket.timeout.ms': 5000,
                'session.timeout.ms': 6000,
            }
            self.consumer = Consumer(conf)
            self.consumer.subscribe([config.kafka_topic_metrics])
            print(f"✅ Kafka consumer subscribed to {config.kafka_topic_metrics}")
            return True
        except Exception as e:
            print(f"⚠️ Kafka consumer error: {e}")
            self.consumer = None
            return False
    
    def register_callback(self, callback: Callable):
        """Register a callback function for incoming messages"""
        self.callbacks.append(callback)
    
    def consume_messages(self):
        """Main consumption loop"""
        print("🎧 Listening for Kafka messages...")
        
        if not self.consumer:
            print("⚠️ No Kafka consumer - running in demo mode")
            self._demo_mode()
            return
        
        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        continue
                
                # Process the message
                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    pod_key = event.get('pod_key')
                    
                    if pod_key:
                        self.latest_metrics[pod_key] = {
                            'cpu': event.get('cpu', 0),
                            'memory_mb': event.get('memory_mb', 0),
                            'io_kbps': event.get('io_kbps', 0),
                            'namespace': event.get('namespace', ''),
                            'pod_name': event.get('pod_name', ''),
                            'timestamp': datetime.now()
                        }
                        self.message_count += 1
                        
                        if self.message_count % 10 == 0:
                            print(f"📥 Received {self.message_count} messages, tracking {len(self.latest_metrics)} pods")
                        
                        # Notify callbacks
                        for callback in self.callbacks:
                            try:
                                callback(event)
                            except Exception as e:
                                print(f"Callback error: {e}")
                                
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Consumer error: {e}")
                time.sleep(1)
    
    def _demo_mode(self):
        """Fallback when Kafka is not available - generates simulated metrics"""
        print("📡 Demo mode - generating simulated metrics for ESP pods")
        
        import random
        
        # Simulated ESP pods
        esp_pods = [
            {'namespace': 'town-traffic', 'name': 'esp-traffic-camera'},
            {'namespace': 'campus-water', 'name': 'esp-water-sensor'},
            {'namespace': 'kitchen-traffic', 'name': 'esp-smart-kitchen'}
        ]
        
        while self.running:
            for pod in esp_pods:
                pod_key = f"{pod['namespace']}/{pod['name']}"
                
                # Generate realistic metrics with occasional spikes
                cpu = random.uniform(0.2, 0.6)
                memory = random.uniform(150, 400)
                
                # 10% chance of anomaly
                if random.random() < 0.1:
                    cpu = random.uniform(0.8, 1.2)
                    memory = random.uniform(600, 1200)
                    print(f"⚠️ SIMULATED ANOMALY: {pod['name']} CPU={cpu:.2f}")
                
                self.latest_metrics[pod_key] = {
                    'cpu': cpu,
                    'memory_mb': memory,
                    'io_kbps': random.uniform(100, 2000),
                    'namespace': pod['namespace'],
                    'pod_name': pod['name'],
                    'timestamp': datetime.now()
                }
                self.message_count += 1
            
            time.sleep(3)  # Update every 3 seconds
    
    def get_latest_metrics(self) -> Dict:
        """Get the latest metrics for all pods"""
        return self.latest_metrics
    
    def start(self):
        """Start consumer in background thread"""
        thread = threading.Thread(target=self.consume_messages, daemon=True)
        thread.start()
        return thread
    
    def stop(self):
        """Stop the consumer"""
        self.running = False
        if self.consumer:
            self.consumer.close()

# Global consumer instance
kafka_consumer = MetricsKafkaConsumer()
