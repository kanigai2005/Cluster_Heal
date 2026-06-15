# kafka_producer_working.py - Fixed Prometheus queries
import json
import time
import requests
import threading
from confluent_kafka import Producer
from config import config

class WorkingKafkaProducer:
    def __init__(self):
        self.producer = None
        self.running = True
        self.last_values = {}
        self._connect()
    
    def _connect(self):
        try:
            conf = {
                'bootstrap.servers': config.kafka_bootstrap_servers,
                'client.id': 'sre-producer',
                'compression.type': 'gzip',
            }
            self.producer = Producer(conf)
            print(f"✅ Kafka producer connected to {config.kafka_bootstrap_servers}")
            return True
        except Exception as e:
            print(f"⚠️ Kafka error: {e}")
            self.producer = None
            return False
    
    def get_esp_metrics(self):
        """Get metrics for ESP pods from Prometheus"""
        # Query for CPU metrics of ESP pods
        cpu_query = 'sum(rate(container_cpu_usage_seconds_total{pod=~"esp.*"}[1m])) by (pod, namespace)'
        
        # Query for memory metrics
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
        print("🚀 Starting metric streaming to Kafka...")
        print(f"   Looking for ESP pods in Prometheus")
        
        while self.running:
            try:
                metrics = self.get_esp_metrics()
                
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
                            print(f"📤 {pod_key}: CPU={cpu:.3f}, Mem={mem:.0f}MB")
                    
                    self.producer.flush()
                    print(f"   Total: {len(metrics)} ESP pods")
                
                time.sleep(5)
                
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(5)
    
    def start(self):
        thread = threading.Thread(target=self.stream_metrics, daemon=True)
        thread.start()
        return thread

kafka_producer = WorkingKafkaProducer()
