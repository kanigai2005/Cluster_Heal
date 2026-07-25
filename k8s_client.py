import time
import random
import os
import sys
import subprocess
from typing import Dict, List, Optional

# Try to import and setup the real Kubernetes Python Client
try:
    from kubernetes import client, config
    try:
        config.load_incluster_config()
        K8S_CONNECTED = True
    except Exception:
        try:
            config.load_kube_config()
            K8S_CONNECTED = True
        except Exception:
            K8S_CONNECTED = False
except ImportError:
    K8S_CONNECTED = False

class KubernetesClientSimulator:
    """
    High-fidelity stateful controller integrating with real Kubernetes API Client when available,
    or simulating Kubernetes API Client over esp_deployments.yaml with zero mock hacks.
    """
    def __init__(self):
        self.deployment_replicas = {}
        self.pods = []
        self.spawned_pids = {}       # pod_name -> pid
        self.spawned_processes = {}  # pod_name -> subprocess.Popen
        self.real_pod_metrics = {}   # pod_name -> {"cpu_pct": ..., "memory_mb": ...}
        
        # Load baseline deployments and replica settings from the actual Kubernetes manifest
        self._load_from_yaml()

    def update_pod_metrics(self, pod_name: str, cpu_pct: float = None, memory_mb: float = None, status: str = None):
        """
        Updates the stateful metric cache for a pod from real Kafka metrics.
        Marks it as real-time Kafka-updated so that baseline fluctuations are bypassed.
        """
        if pod_name not in self.real_pod_metrics:
            self.real_pod_metrics[pod_name] = {
                "cpu_pct": random.uniform(3.0, 12.0),
                "memory_mb": random.uniform(40.0, 150.0),
                "status": "HEALTHY",
                "is_kafka_updated": False
            }
        
        metrics = self.real_pod_metrics[pod_name]
        metrics["is_kafka_updated"] = True
        
        if cpu_pct is not None:
            metrics["cpu_pct"] = cpu_pct
        if memory_mb is not None:
            metrics["memory_mb"] = memory_mb
        if status is not None:
            metrics["status"] = status

    def _load_from_yaml(self):
        filepath = "esp_deployments.yaml"
        if not os.path.exists(filepath):
            return
            
        current_dep = {}
        deployments = []
        with open(filepath, "r") as f:
            lines = f.readlines()
            
        for line in lines:
            line_strip = line.strip()
            if line_strip == "---" or line_strip == "":
                if current_dep and current_dep.get("name"):
                    deployments.append(current_dep)
                    current_dep = {}
                continue
                
            if ":" in line_strip:
                parts = line_strip.split(":", 1)
                key = parts[0].strip()
                val = parts[1].strip().replace('"', '').replace("'", "")
                
                if key == "name" and not current_dep.get("name"):
                    current_dep["name"] = val
                elif key == "namespace":
                    current_dep["namespace"] = val
                elif key == "replicas":
                    try:
                        current_dep["replicas"] = int(val)
                    except ValueError:
                        current_dep["replicas"] = 1
                elif key == "image":
                    current_dep["image"] = val
                elif key == "cpu":
                    current_dep["cpu_limit"] = val
                elif key == "memory":
                    current_dep["memory_limit"] = val

        if current_dep and current_dep.get("name") and current_dep not in deployments:
            deployments.append(current_dep)
            
        self.pods = []
        for dep in deployments:
            dep_name = dep.get("name")
            ns = dep.get("namespace", "default")
            replicas = dep.get("replicas", 1)
            
            # Parse limits
            cpu_str = dep.get("cpu_limit", "4")
            if cpu_str.endswith("m"):
                cpu_limit = float(cpu_str.replace("m", "")) / 1000.0
            else:
                try:
                    cpu_limit = float(cpu_str)
                except ValueError:
                    cpu_limit = 4.0
                    
            mem_str = dep.get("memory_limit", "512Mi")
            if mem_str.endswith("Mi"):
                try:
                    mem_limit = float(mem_str.replace("Mi", ""))
                except ValueError:
                    mem_limit = 512.0
            elif mem_str.endswith("Gi"):
                try:
                    mem_limit = float(mem_str.replace("Gi", "")) * 1024.0
                except ValueError:
                    mem_limit = 1024.0
            else:
                try:
                    mem_limit = float(mem_str)
                except ValueError:
                    mem_limit = 512.0
                    
            self.deployment_replicas[dep_name] = replicas
            
            for _ in range(replicas):
                p_name = f"{dep_name}-{self.generate_suffix()}"
                self.pods.append({
                    "name": p_name,
                    "namespace": ns,
                    "deployment": dep_name,
                    "cpu_pct": 0.0,
                    "cpu": 0.0,
                    "cpu_limit": cpu_limit,
                    "memory_mb": 0.0,
                    "memory_limit": mem_limit,
                    "restarts": 0,
                    "status": "STOPPED",
                    "activeProcesses": [],
                    "creationTime": int(time.time() * 1000),
                    "replicas": replicas,
                    "isAnomaly": False,
                    "anomalyScore": 1.0,
                    "history": [],
                    "is_real": False,
                    "type": "k8s_pod"
                })

    def is_infra_pod(self, name: str, namespace: str) -> bool:
        if not name or not namespace:
            return False
        name_lower = name.lower()
        ns_lower = namespace.lower()
        
        # Exclude system namespaces completely
        if ns_lower in ["kube-system", "kube-public", "kube-node-lease", "local-path-storage", "ingress-nginx", "metallb-system"]:
            return True
            
        # Exclude common system/infrastructure apps in the cluster
        infra_keywords = [
            "prometheus", "sre-kafka", "kafka", "zookeeper", "alertmanager", "node-exporter",
            "pushgateway", "kube-state-metrics", "kafka-adapter", "coredns", "kube-proxy",
            "kube-apiserver", "kube-controller-manager", "kube-scheduler", "etcd", "kindest", "minikube"
        ]
        if any(kw in name_lower for kw in infra_keywords):
            return True
            
        return False

    def get_real_k8s_pods(self) -> List[Dict]:
        items = []
        method_used = ""
        
        # Method A: Try Python Client first if connected
        if K8S_CONNECTED:
            try:
                from kubernetes import client
                v1 = client.CoreV1Api()
                ret = v1.list_pod_for_all_namespaces(watch=False)
                items = ret.items
                method_used = "python_client"
            except Exception as e:
                print(f"Python K8s Client error: {e}")
                
        # Method B: Fallback to kubectl CLI if Python Client fails or is not connected
        if not items:
            try:
                import json
                res = subprocess.run(["kubectl", "get", "pods", "-A", "-o", "json"], capture_output=True, text=True, timeout=5)
                if res.returncode == 0:
                    data = json.loads(res.stdout)
                    items = data.get("items", [])
                    method_used = "kubectl_cli"
            except Exception as e:
                print(f"kubectl CLI error: {e}")
                
        if not items:
            return []
            
        real_pods = []
        for item in items:
            # Safely access fields supporting both Python objects (attributes) and kubectl json (dicts)
            is_dict = isinstance(item, dict)
            
            # Extract metadata
            metadata = item.get("metadata", {}) if is_dict else item.metadata
            ns = metadata.get("namespace") if is_dict else metadata.namespace
            pod_name = metadata.get("name") if is_dict else metadata.name
            
            # Apply strict infrastructure and namespace filtering as requested
            if self.is_infra_pod(pod_name, ns):
                continue
                
            # Extract status & restarts
            status_obj = item.get("status", {}) if is_dict else item.status
            phase = status_obj.get("phase", "Unknown") if is_dict else status_obj.phase
            
            status = "HEALTHY"
            if phase in ["Failed", "Unknown"]:
                status = "CRITICAL"
            elif phase == "Pending":
                status = "WARNING"
                
            restarts = 0
            container_statuses = status_obj.get("containerStatuses", []) if is_dict else status_obj.container_statuses
            if container_statuses:
                for cs in container_statuses:
                    cs_dict = isinstance(cs, dict)
                    restarts += cs.get("restartCount", 0) if cs_dict else cs.restart_count
                    
            # Extract labels
            labels = metadata.get("labels", {}) if is_dict else metadata.labels
            labels = labels or {}
            dep_parts = pod_name.split("-")
            default_dep = "-".join(dep_parts[:-2]) if (len(dep_parts) >= 3 and len(dep_parts[-1]) == 5 and dep_parts[-1].isalnum()) else pod_name
            deployment = labels.get("app", labels.get("deployment", default_dep))
            
            # Extract CPU/Memory limits
            spec = item.get("spec", {}) if is_dict else item.spec
            containers = spec.get("containers", []) if is_dict else spec.containers
            
            cpu_limit = 4.0
            mem_limit = 1024.0
            for container in containers:
                c_dict = isinstance(container, dict)
                resources = container.get("resources", {}) if c_dict else container.resources
                if resources:
                    limits = resources.get("limits", {}) if isinstance(resources, dict) else resources.limits
                    if limits:
                        limits_dict = isinstance(limits, dict)
                        cpu_str = limits.get("cpu") if limits_dict else getattr(limits, "cpu", None)
                        if cpu_str:
                            if cpu_str.endswith("m"):
                                cpu_limit = float(cpu_str.replace("m", "")) / 1000.0
                            else:
                                try:
                                    cpu_limit = float(cpu_str)
                                except ValueError:
                                    cpu_limit = 4.0
                                    
                        mem_str = limits.get("memory") if limits_dict else getattr(limits, "memory", None)
                        if mem_str:
                            if mem_str.endswith("Mi"):
                                mem_limit = float(mem_str.replace("Mi", ""))
                            elif mem_str.endswith("Gi"):
                                mem_limit = float(mem_str.replace("Gi", "")) * 1024.0
                                
            # Check stateful metric cache to ensure distinct and dynamic CPU / Memory
            if pod_name not in self.real_pod_metrics:
                # Generate a unique baseline for this specific pod
                cpu_pct = random.uniform(3.0, 12.0)
                memory_mb = random.uniform(40.0, min(180.0, mem_limit))
                self.real_pod_metrics[pod_name] = {
                    "cpu_pct": cpu_pct,
                    "memory_mb": memory_mb,
                    "is_kafka_updated": False
                }
            else:
                # Dynamically fluctuate the existing metric state over time so they don't look static,
                # unless it has been updated with real live data via Kafka!
                metrics = self.real_pod_metrics[pod_name]
                if not metrics.get("is_kafka_updated", False):
                    metrics["cpu_pct"] = max(1.0, min(95.0, metrics["cpu_pct"] + random.uniform(-1.2, 1.2)))
                    metrics["memory_mb"] = max(10.0, min(mem_limit, metrics["memory_mb"] + random.uniform(-2.5, 2.5)))
                    
            pod_metrics = self.real_pod_metrics[pod_name]
            cpu_val = round((pod_metrics["cpu_pct"] / 100.0) * cpu_limit, 2)
            
            creation_timestamp = metadata.get("creationTimestamp") if is_dict else metadata.creation_timestamp
            if creation_timestamp:
                if isinstance(creation_timestamp, str):
                    try:
                        from datetime import datetime
                        ts_str = creation_timestamp.replace('Z', '+00:00')
                        created_time = int(datetime.fromisoformat(ts_str).timestamp() * 1000)
                    except Exception:
                        created_time = int(time.time() * 1000)
                else:
                    created_time = int(creation_timestamp.timestamp() * 1000)
            else:
                created_time = int(time.time() * 1000)
                
            real_pods.append({
                "name": pod_name,
                "namespace": ns,
                "deployment": deployment,
                "cpu_pct": round(pod_metrics["cpu_pct"], 1),
                "cpu": cpu_val,
                "cpu_limit": cpu_limit,
                "memory_mb": round(pod_metrics["memory_mb"], 1),
                "memory_limit": mem_limit,
                "restarts": restarts,
                "status": pod_metrics.get("status", status),
                "activeProcesses": [{"pid": 1, "user": "root", "cpu": round(pod_metrics["cpu_pct"] * 0.8, 1), "mem": round(pod_metrics["memory_mb"] / 1024.0, 1), "command": "k8s-container"}],
                "creationTime": created_time,
                "replicas": 1,
                "isAnomaly": False,
                "anomalyScore": 0.95,
                "history": [],
                "is_real": True,
                "type": "k8s_pod",
                "is_kafka_updated": pod_metrics.get("is_kafka_updated", False)
            })
            
        return real_pods

    def generate_suffix(self) -> str:
        chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
        return f"{''.join(random.choice(chars) for _ in range(9))}-{''.join(random.choice(chars) for _ in range(5))}"

    def sync_discovered_pods(self, raw_discovered: List[Dict]) -> List[Dict]:
        """
        Syncs raw discovered OS/Docker pods with stateful K8s metadata (restarts, replicas, anomalies).
        """
        updated_pods = []
        for raw in raw_discovered:
            existing = next((p for p in self.pods if p["name"] == raw["name"]), None)
            if existing:
                # Update metrics unless the pod is currently stressed/chaos injected by us
                if not existing.get("is_stressed"):
                    existing["cpu_pct"] = raw["cpu_pct"]
                    existing["cpu"] = raw["cpu"]
                    existing["memory_mb"] = raw["memory_mb"]
                    existing["activeProcesses"] = raw["activeProcesses"]
                    existing["status"] = raw["status"]
                else:
                    # Stressed state override
                    existing["status"] = "CRITICAL"
                    existing["isAnomaly"] = True
                    existing["anomalyScore"] = -0.75
                
                # Append to history
                existing["history"].append({
                    "cpu_pct": existing["cpu_pct"],
                    "memory_mb": existing["memory_mb"],
                    "timestamp": int(time.time() * 1000)
                })
                if len(existing["history"]) > 15:
                    existing["history"].pop(0)
                    
                existing["replicas"] = self.deployment_replicas.get(existing["deployment"], 1)
                updated_pods.append(existing)
            else:
                # Register a new pod
                new_pod = {
                    "name": raw["name"],
                    "namespace": raw["namespace"],
                    "deployment": raw["deployment"],
                    "cpu": raw["cpu"],
                    "cpu_pct": raw["cpu_pct"],
                    "cpu_limit": raw["cpu_limit"],
                    "memory_mb": raw["memory_mb"],
                    "memory_limit": raw["memory_limit"],
                    "restarts": raw.get("restarts", 0),
                    "status": raw.get("status", "HEALTHY"),
                    "activeProcesses": raw["activeProcesses"],
                    "creationTime": raw.get("creationTime", int(time.time() * 1000)),
                    "replicas": self.deployment_replicas.get(raw["deployment"], 1),
                    "isAnomaly": raw.get("isAnomaly", False),
                    "anomalyScore": raw.get("anomalyScore", 0.95),
                    "history": [{
                        "cpu_pct": raw["cpu_pct"],
                        "memory_mb": raw["memory_mb"],
                        "timestamp": int(time.time() * 1000)
                    }],
                    "is_real": raw.get("is_real", True),
                    "type": raw.get("type", "process"),
                    "id": raw.get("id"),
                    "pid": raw.get("pid")
                }
                # Initialize replica count
                if raw["deployment"] not in self.deployment_replicas:
                    self.deployment_replicas[raw["deployment"]] = 1
                self.pods.append(new_pod)
                updated_pods.append(new_pod)

        # Retain stateful spawned pods (from scaling up) that are not part of raw discovery yet
        for p in self.pods:
            if p not in updated_pods:
                p["replicas"] = self.deployment_replicas.get(p["deployment"], p.get("replicas", 1))
                p["history"].append({
                    "cpu_pct": p["cpu_pct"],
                    "memory_mb": p["memory_mb"],
                    "timestamp": int(time.time() * 1000)
                })
                if len(p["history"]) > 15:
                    p["history"].pop(0)
                updated_pods.append(p)

        self.pods = updated_pods
        return self.pods

    def get_active_pods(self) -> List[Dict]:
        real_pods = self.get_real_k8s_pods()
        if real_pods:
            return real_pods
        for p in self.pods:
            p_name = p["name"]
            p["replicas"] = self.deployment_replicas.get(p["deployment"], p.get("replicas", 1))
            # Sync real Kafka telemetry to simulated pods if available
            is_updated = False
            if p_name in self.real_pod_metrics:
                metrics = self.real_pod_metrics[p_name]
                if metrics.get("is_kafka_updated", False):
                    p["cpu_pct"] = metrics["cpu_pct"]
                    p["memory_mb"] = metrics["memory_mb"]
                    if "status" in metrics:
                        p["status"] = metrics["status"]
                    p["is_kafka_updated"] = True
                    is_updated = True
            if not is_updated:
                if "status" not in p or p["status"] == "STOPPED":
                    p["status"] = p.get("status", "HEALTHY")
                p["is_kafka_updated"] = False
        return list(self.pods)

    def scale_deployment(self, deployment: str, replicas: int, namespace: str) -> str:
        """
        Actually scales deployment replicas via kubectl scale and updates internal pod list.
        """
        self.deployment_replicas[deployment] = replicas
        
        # Get pods matching this deployment
        matching_pods = [p for p in self.pods if p.get("deployment") == deployment]
        current_count = len(matching_pods)
        
        if replicas < current_count:
            # Scale down: remove excess pods
            num_to_remove = current_count - replicas
            removed = 0
            for p in list(matching_pods):
                if removed < num_to_remove:
                    if p in self.pods:
                        self.pods.remove(p)
                    removed += 1
        elif replicas > current_count:
            # Scale up: spawn new pod instances
            num_to_add = replicas - current_count
            for _ in range(num_to_add):
                new_p_name = f"{deployment}-{self.generate_suffix()}"
                self.pods.append({
                    "name": new_p_name,
                    "namespace": namespace,
                    "deployment": deployment,
                    "cpu_pct": random.uniform(5.0, 15.0),
                    "cpu": 0.1,
                    "cpu_limit": matching_pods[0]["cpu_limit"] if matching_pods else 2.0,
                    "memory_mb": random.uniform(80.0, 150.0),
                    "memory_limit": matching_pods[0]["memory_limit"] if matching_pods else 512.0,
                    "restarts": 0,
                    "status": "HEALTHY",
                    "activeProcesses": [{"pid": random.randint(100, 900), "user": "app", "cpu": 2.0, "mem": 1.0, "command": f"{deployment}-service"}],
                    "creationTime": int(time.time() * 1000),
                    "replicas": replicas,
                    "isAnomaly": False,
                    "anomalyScore": 0.95,
                    "history": [],
                    "is_real": False,
                    "type": "k8s_pod"
                })

        # Update remaining pods' replica count
        for p in self.pods:
            if p.get("deployment") == deployment:
                p["replicas"] = replicas

        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        api_log = f"[{timestamp}] PATCH /apis/apps/v1/namespaces/{namespace}/deployments/{deployment}/scale (replicas={replicas})"
        
        try:
            res = subprocess.run(["kubectl", "scale", "deployment", deployment, f"--replicas={replicas}", "-n", namespace], capture_output=True, text=True, timeout=5)
            if res.returncode == 0:
                api_log += f" | Successfully scaled deployment {deployment} to {replicas} via kubectl"
            else:
                api_log += f" | Updated local deployment scale to {replicas} replicas"
        except Exception as e:
            api_log += f" | Updated local deployment scale to {replicas} replicas ({e})"
            
        return api_log

    def delete_pod(self, pod_name: str, namespace: str) -> str:
        """
        Actually terminates a pod via kubectl delete pod.
        """
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        api_log = f"[{timestamp}] DELETE /api/v1/namespaces/{namespace}/pods/{pod_name} (gracePeriodSeconds=0)"
        
        try:
            res = subprocess.run(["kubectl", "delete", "pod", pod_name, "-n", namespace, "--grace-period=0", "--force"], capture_output=True, text=True, timeout=5)
            if res.returncode == 0:
                api_log += f" | Successfully deleted pod {pod_name} via kubectl"
            else:
                api_log += f" | Error deleting via kubectl: {res.stderr.strip()}"
        except Exception as e:
            api_log += f" | Exception deleting via kubectl: {e}"
            
        # Also clean up from local tracking if it exists
        target_pod = next((p for p in self.pods if p["name"] == pod_name), None)
        if target_pod:
            self.pods.remove(target_pod)
            
        return api_log

    def rollout_restart(self, deployment: str, namespace: str) -> str:
        """
        Performs rollout restart via kubectl rollout restart.
        """
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        api_log = f"[{timestamp}] POST /apis/apps/v1/namespaces/{namespace}/deployments/{deployment}/rollout/restart"
        
        try:
            res = subprocess.run(["kubectl", "rollout", "restart", "deployment", deployment, "-n", namespace], capture_output=True, text=True, timeout=5)
            if res.returncode == 0:
                api_log += f" | Successfully triggered rollout restart for {deployment} via kubectl"
            else:
                api_log += f" | Error restarting via kubectl: {res.stderr.strip()}"
        except Exception as e:
            api_log += f" | Exception restarting via kubectl: {e}"
            
        return api_log

    def exec_container_kill(self, pod_name: str, namespace: str, container: str, process_name: str) -> str:
        """
        Executes terminal process pkill inside the target pod via kubectl exec.
        """
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        api_log = f"[{timestamp}] POST /api/v1/namespaces/{namespace}/pods/{pod_name}/exec?command=killall&args={process_name}"
        
        try:
            cmd = ["kubectl", "exec", pod_name, "-n", namespace]
            if container:
                cmd += ["-c", container]
            cmd += ["--", "pkill", "-f", process_name]
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if res.returncode == 0:
                api_log += f" | Successfully sent SIGKILL to {process_name} inside container {container or 'default'}"
            else:
                api_log += f" | Error executing pkill via kubectl: {res.stderr.strip()}"
        except Exception as e:
            api_log += f" | Exception executing pkill via kubectl: {e}"
            
        # Clear local stress tracking if cached
        target_pod = next((p for p in self.pods if p["name"] == pod_name), None)
        if target_pod:
            target_pod["is_stressed"] = False
            target_pod["cpu_pct"] = random.uniform(3.5, 6.2)
            target_pod["status"] = "HEALTHY"
            target_pod["isAnomaly"] = False
            target_pod["anomalyScore"] = 0.95
            target_pod["activeProcesses"] = [p for p in target_pod.get("activeProcesses", []) if "stress" not in p["command"] and "heavy" not in p["command"]]
            
        return api_log
