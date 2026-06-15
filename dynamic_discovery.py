# dynamic_discovery.py - Auto-discovers pods in real-time
import subprocess
import json
import threading
import time
from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass, field

@dataclass
class PodInfo:
    name: str
    namespace: str
    node: str
    status: str
    restarts: int
    creation_time: str
    labels: Dict[str, str] = field(default_factory=dict)

class DynamicPodDiscovery:
    """
    Dynamically discovers ALL pods in the cluster.
    Runs in background and updates automatically.
    """
    
    def __init__(self):
        self.pods: Dict[str, PodInfo] = {}
        self.listeners = []
        self.running = True
        self.last_update = None
        
        # Start discovery thread
        self.start_discovery()
    
    def discover_pods(self) -> Dict[str, PodInfo]:
        """Query Kubernetes API for all pods"""
        try:
            # Get all pods across all namespaces
            cmd = "kubectl get pods --all-namespaces -o json"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            print("result::",result)
            if result.returncode != 0:
                return self.pods
            
            data = json.loads(result.stdout)
            print("data::",data)
            new_pods = {}
            
            for item in data.get('items', []):
                print("item::",item)
                metadata = item.get('metadata', {})
                spec = item.get('spec', {})
                status = item.get('status', {})
                
                name = metadata.get('name')
                namespace = metadata.get('namespace')
                
                # Skip system pods
                if namespace in ['kube-system', 'local-path-storage', 'cert-manager']:
                    continue
                
                # Calculate restarts
                restarts = 0
                for cs in status.get('containerStatuses', []):
                    restarts += cs.get('restartCount', 0)
                
                key = f"{namespace}/{name}"
                new_pods[key] = PodInfo(
                    name=name,
                    namespace=namespace,
                    node=spec.get('nodeName', 'unknown'),
                    status=status.get('phase', 'Unknown'),
                    restarts=restarts,
                    creation_time=metadata.get('creationTimestamp', ''),
                    labels=metadata.get('labels', {})
                )
            
            # Detect changes
            old_keys = set(self.pods.keys())
            new_keys = set(new_pods.keys())
            
            added = new_keys - old_keys
            removed = old_keys - new_keys
            
            if added or removed:
                print(f"🔄 Pod discovery updated: +{len(added)} -{len(removed)} (Total: {len(new_pods)})")
                for key in added:
                    pod = new_pods[key]
                    print(f"  ➕ New pod: {key} on {pod.node}")
                
                # Notify listeners
                for listener in self.listeners:
                    listener('UPDATE', {'added': list(added), 'removed': list(removed), 'pods': new_pods})
            
            self.pods = new_pods
            self.last_update = datetime.now()
            
        except Exception as e:
            print(f"Discovery error: {e}")
        
        return self.pods
    
    def get_pod_metrics_targets(self) -> List[Dict]:
        """Get list of pods to monitor (for Prometheus queries)"""
        targets = []
        for key, pod in self.pods.items():
            # Only monitor Running pods
            if pod.status == 'Running':
                targets.append({
                    'pod_key': key,
                    'namespace': pod.namespace,
                    'pod_name': pod.name,
                    'node': pod.node
                })
        return targets
    
    def register_listener(self, callback):
        """Register callback for pod changes"""
        self.listeners.append(callback)
    
    def start_discovery(self):
        """Background thread for continuous discovery"""
        def discover_loop():
            while self.running:
                self.discover_pods()
                time.sleep(30)  # Discover every 30 seconds
        
        thread = threading.Thread(target=discover_loop, daemon=True)
        thread.start()
        print("🔍 Dynamic pod discovery started (interval: 30s)")

# Global instance
pod_discovery = DynamicPodDiscovery()
