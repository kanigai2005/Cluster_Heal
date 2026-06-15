# kafka_producer.py - Kafka producer for sending metrics
import json
import time
import requests
import threading
from confluent_kafka import Producer
from config import config

class MetricsKafkaProducer:
    """
    Fetches metrics from Prometheus and sends to Kafka.
    """
    
    def __init__(self):
        self.producer = None
        self.running = True
        self.message_count = 0
        self._connect()
    
    def _connect(self):
        """Connect to Kafka broker"""
        try:
            conf = {
                'bootstrap.servers': config.kafka_bootstrap_servers,
                'client.id': 'sre-producer',
                'compression.type': 'gzip',
                'acks': '1',
                'socket.timeout.ms': 5000,
            }
            self.producer = Producer(conf)
            print(f"✅ Kafka producer connected to {config.kafka_bootstrap_servers}")
            return True
        except Exception as e:
            print(f"⚠️ Kafka not available: {e}")
            print("Running in demo mode (no Kafka)")
            self.producer = None
            return False
    
    def fetch_esp_metrics(self):
        """Fetch metrics for ESP pods from Prometheus"""
        # Query for CPU metrics of ESP pods
        cpu_query = 'sum(rate(container_cpu_usage_seconds_total{pod=~"esp.*"}[1m])) by (pod, namespace)'
        mem_query = 'sum(container_memory_working_set_bytes{pod=~"esp.*"}) by (pod, namespace)'
        
        metrics = {}
        
        try:
            # Get CPU metrics
            resp = requests.get(f"{config.prometheus_url}/api/v1/query", 
                               params={'query': cpu_query}, timeout=5)
            data = resp.json()
            
            if data.get('status') == 'success':
                for item in data['data']['result']:
                    pod = item['metric'].get('pod', '')
                    namespace = item['metric'].get('namespace', '')
                    cpu_value = float(item['value'][1]) if item['value'][1] != 'NaN' else 0.0
                    
                    key = f"{namespace}/{pod}"
                    if key not in metrics:
                        metrics[key] = {'namespace': namespace, 'pod': pod}
                    metrics[key]['cpu'] = cpu_value
            
            # Get memory metrics
            resp = requests.get(f"{config.prometheus_url}/api/v1/query",
                               params={'query': mem_query}, timeout=5)
            data = resp.json()
            
            if data.get('status') == 'success':
                for item in data['data']['result']:
                    pod = item['metric'].get('pod', '')
                    namespace = item['metric'].get('namespace', '')
                    mem_value = float(item['value'][1]) / (1024 * 1024) if item['value'][1] != 'NaN' else 0.0
                    
                    key = f"{namespace}/{pod}"
                    if key in metrics:
                        metrics[key]['memory_mb'] = mem_value
                    else:
                        metrics[key] = {'namespace': namespace, 'pod': pod, 'memory_mb': mem_value}
            
            return metrics
            
        except Exception as e:
            print(f"Prometheus query error: {e}")
            return {}
    
    def stream_metrics(self):
        """Main streaming loop"""
        print("🚀 Starting metric streaming to Kafka...")
        print(f"   Looking for ESP pods in Prometheus at {config.prometheus_url}")
        
        while self.running:
            try:
                metrics = self.fetch_esp_metrics()
                
                if metrics and self.producer:
                    for pod_key, data in metrics.items():
                        cpu = data.get('cpu', 0)
                        mem = data.get('memory_mb', 0)
                        
                        # Only send if there's actual CPU usage
                        if cpu > 0.001 or mem > 0:
                            event = {
                                'timestamp': time.time(),
                                'pod_key': pod_key,
                                'namespace': data['namespace'],
                                'pod_name': data['pod'],
                                'cpu': round(cpu, 4),
                                'memory_mb': round(mem, 1),
                                'io_kbps': 0
                            }
                            
                            self.producer.produce(
                                config.kafka_topic_metrics,
                                value=json.dumps(event).encode('utf-8')
                            )
                            self.producer.poll(0)
                            self.message_count += 1
                            
                            if self.message_count % 5 == 0:
                                print(f"📤 Sent {self.message_count} messages to Kafka")
                    
                    if self.producer:
                        self.producer.flush()
                
                # If no metrics from Prometheus, generate simulated ones
                elif not metrics:
                    self._generate_simulated_metrics()
                
                time.sleep(config.refresh_interval)
                
            except Exception as e:
                print(f"Streaming error: {e}")
                time.sleep(5)
    
    def _generate_simulated_metrics(self):
        """Generate simulated metrics when Prometheus has no data"""
        import random
        
        esp_pods = [
            {'namespace': 'town-traffic', 'name': 'esp-traffic-camera'},
            {'namespace': 'campus-water', 'name': 'esp-water-sensor'},
            {'namespace': 'kitchen-traffic', 'name': 'esp-smart-kitchen'}
        ]
        
        for pod in esp_pods:
            pod_key = f"{pod['namespace']}/{pod['name']}"
            cpu = random.uniform(0.2, 0.6)
            memory = random.uniform(150, 400)
            
            # Occasional spike
            if random.random() < 0.05:
                cpu = random.uniform(0.8, 1.2)
                memory = random.uniform(600, 1200)
            
            event = {
                'timestamp': time.time(),
                'pod_key': pod_key,
                'namespace': pod['namespace'],
                'pod_name': pod['name'],
                'cpu': round(cpu, 4),
                'memory_mb': round(memory, 1),
                'io_kbps': 0
            }
            
            if self.producer:
                self.producer.produce(
                    config.kafka_topic_metrics,
                    value=json.dumps(event).encode('utf-8')
                )
                self.producer.poll(0)
        
        if self.producer:
            self.producer.flush()
        print(f"📤 Generated simulated metrics for {len(esp_pods)} ESP pods")
    
    def start(self):
        """Start producer in background thread"""
        thread = threading.Thread(target=self.stream_metrics, daemon=True)
        thread.start()
        return thread
    
    def stop(self):
        """Stop the producer"""
        self.running = False
        if self.producer:
            self.producer.flush()

# Global producer instance
kafka_producer = MetricsKafkaProducer()
