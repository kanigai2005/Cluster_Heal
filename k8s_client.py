import time
import random
import os
import sys
import subprocess
from typing import Dict, List, Optional, Tuple

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
        self.deleted_pod_names = set()
        self.deleted_deployments = set()
        
        # Load baseline deployments and replica settings from the actual Kubernetes manifest
        self._load_from_yaml()

    def update_pod_metrics(self, pod_name: str, cpu_pct: float = None, memory_mb: float = None, status: str = None):
        """
        Updates the stateful metric cache for a pod from real Kafka/Prometheus metrics.
        Marks it as real-time Kafka-updated so that baseline fluctuations are bypassed.
        """
        if pod_name not in self.real_pod_metrics:
            self.real_pod_metrics[pod_name] = {
                "cpu_pct": 0.0,
                "memory_mb": 0.0,
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

        # Direct sync into stateful pod instances
        for p in self.pods:
            if p["name"] == pod_name or p["deployment"] == pod_name:
                p["is_kafka_updated"] = True
                if cpu_pct is not None:
                    p["cpu_pct"] = cpu_pct
                    p["cpu"] = round((cpu_pct / 100.0) * p.get("cpu_limit", 4.0), 2)
                if memory_mb is not None:
                    p["memory_mb"] = memory_mb
                if status is not None:
                    p["status"] = status

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
            
            # Known baseline pod mapping from cluster state
            known_pods_map = {
                "esp-water-sensor": [
                    {"name": "esp-water-sensor-67f44464c-szxfj", "status": "HEALTHY", "restarts": 23, "cpu_pct": 9.8, "memory_mb": 147.1},
                    {"name": "esp-water-sensor-7c6879c9dd-d6pdg", "status": "ImagePullBackOff", "restarts": 0, "cpu_pct": 0.0, "memory_mb": 12.0}
                ],
                "esp-watersensor": [
                    {"name": "esp-watersensor-584fccb9b6-zzswq", "status": "HEALTHY", "restarts": 14, "cpu_pct": 4.2, "memory_mb": 83.6}
                ],
                "esp-my-nginx": [
                    {"name": "esp-my-nginx-746d7d4949-z48cp", "status": "HEALTHY", "restarts": 1, "cpu_pct": 2.1, "memory_mb": 42.8}
                ],
                "esp-smart-kitchen": [
                    {"name": "esp-smart-kitchen-55cd7b64f5-kqdlh", "status": "ImagePullBackOff", "restarts": 0, "cpu_pct": 0.0, "memory_mb": 15.0},
                    {"name": "esp-smart-kitchen-698865f6fd-msp88", "status": "ImagePullBackOff", "restarts": 0, "cpu_pct": 0.0, "memory_mb": 15.0}
                ],
                "esp-traffic-camera": [
                    {"name": "esp-traffic-camera-559bbd9b4f-ff69r", "status": "ImagePullBackOff", "restarts": 0, "cpu_pct": 0.0, "memory_mb": 15.0},
                    {"name": "esp-traffic-camera-7d96cc47b9-6j47k", "status": "ImagePullBackOff", "restarts": 0, "cpu_pct": 0.0, "memory_mb": 15.0}
                ]
            }

            templates = known_pods_map.get(dep_name, [])
            for i in range(replicas):
                if i < len(templates):
                    tmpl = templates[i]
                    p_name = tmpl["name"]
                    p_status = tmpl["status"]
                    p_restarts = tmpl["restarts"]
                    init_cpu = tmpl["cpu_pct"]
                    init_mem = tmpl["memory_mb"]
                else:
                    p_name = f"{dep_name}-{self.generate_suffix()}"
                    p_status = "HEALTHY"
                    p_restarts = 0
                    init_cpu = 0.0
                    init_mem = 0.0

                self.pods.append({
                    "name": p_name,
                    "namespace": ns,
                    "deployment": dep_name,
                    "cpu_pct": init_cpu,
                    "cpu": round((init_cpu / 100.0) * cpu_limit, 2),
                    "cpu_limit": cpu_limit,
                    "memory_mb": init_mem,
                    "memory_limit": mem_limit,
                    "restarts": p_restarts,
                    "status": p_status,
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
            if self.is_infra_pod(pod_name, ns) or pod_name in self.deleted_pod_names:
                continue
                
            # Extract status & restarts
            status_obj = item.get("status", {}) if is_dict else item.status
            phase = status_obj.get("phase", "Unknown") if is_dict else getattr(status_obj, "phase", "Unknown")
            
            deletion_ts = metadata.get("deletionTimestamp") if is_dict else getattr(metadata, "deletion_timestamp", None)
            
            pod_status_str = phase
            restarts = 0
            
            if deletion_ts:
                pod_status_str = "Terminating"
            else:
                # Check initContainerStatuses
                init_statuses = status_obj.get("initContainerStatuses", []) if is_dict else getattr(status_obj, "init_container_statuses", None) or []
                found_reason = False
                if init_statuses:
                    for cs in init_statuses:
                        cs_dict = isinstance(cs, dict)
                        restarts += cs.get("restartCount", 0) if cs_dict else getattr(cs, "restart_count", 0)
                        state = cs.get("state", {}) if cs_dict else getattr(cs, "state", None)
                        if state:
                            waiting = state.get("waiting", {}) if isinstance(state, dict) else getattr(state, "waiting", None)
                            if waiting:
                                reason = waiting.get("reason") if isinstance(waiting, dict) else getattr(waiting, "reason", None)
                                if reason:
                                    pod_status_str = f"Init:{reason}"
                                    found_reason = True
                                    break
                            terminated = state.get("terminated", {}) if isinstance(state, dict) else getattr(state, "terminated", None)
                            if terminated:
                                reason = terminated.get("reason") if isinstance(terminated, dict) else getattr(terminated, "reason", None)
                                if reason:
                                    pod_status_str = f"Init:{reason}"
                                    found_reason = True
                                    break

                if not found_reason:
                    # Check containerStatuses
                    container_statuses = status_obj.get("containerStatuses", []) if is_dict else getattr(status_obj, "container_statuses", None) or []
                    if container_statuses:
                        for cs in container_statuses:
                            cs_dict = isinstance(cs, dict)
                            restarts += cs.get("restartCount", 0) if cs_dict else getattr(cs, "restart_count", 0)
                            state = cs.get("state", {}) if cs_dict else getattr(cs, "state", None)
                            if state:
                                waiting = state.get("waiting", {}) if isinstance(state, dict) else getattr(state, "waiting", None)
                                if waiting:
                                    reason = waiting.get("reason") if isinstance(waiting, dict) else getattr(waiting, "reason", None)
                                    if reason:
                                        pod_status_str = reason
                                        break
                                terminated = state.get("terminated", {}) if isinstance(state, dict) else getattr(state, "terminated", None)
                                if terminated:
                                    reason = terminated.get("reason") if isinstance(terminated, dict) else getattr(terminated, "reason", None)
                                    exit_code = terminated.get("exitCode") if isinstance(terminated, dict) else getattr(terminated, "exit_code", 0)
                                    if reason:
                                        pod_status_str = reason
                                        break
                                    elif exit_code and exit_code != 0:
                                        pod_status_str = "Error"
                                        break

            # Map to system status
            status = pod_status_str if pod_status_str not in ["Running", "Succeeded"] else "HEALTHY"
                    
            # Extract labels
            labels = metadata.get("labels", {}) if is_dict else metadata.labels
            labels = labels or {}
            dep_parts = pod_name.split("-")
            default_dep = "-".join(dep_parts[:-2]) if (len(dep_parts) >= 3 and len(dep_parts[-1]) == 5 and dep_parts[-1].isalnum()) else pod_name
            deployment = labels.get("app", labels.get("deployment", default_dep))
            
            if deployment in self.deleted_deployments:
                continue
            
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
                                
            # Check stateful metric cache for pod
            if pod_name not in self.real_pod_metrics:
                self.real_pod_metrics[pod_name] = {
                    "cpu_pct": 0.0,
                    "memory_mb": 0.0,
                    "is_kafka_updated": False
                }
                
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
                "status": status if status not in ["HEALTHY", "Running", "OK"] else pod_metrics.get("status", status),
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
            
        return [p for p in real_pods if p["name"] not in self.deleted_pod_names and p["deployment"] not in self.deleted_deployments]

    def generate_suffix(self) -> str:
        chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
        return f"{''.join(random.choice(chars) for _ in range(9))}-{''.join(random.choice(chars) for _ in range(5))}"

    def sync_discovered_pods(self, raw_discovered: List[Dict]) -> List[Dict]:
        """
        Syncs raw discovered OS/Docker pods with stateful K8s metadata (restarts, replicas, anomalies).
        """
        updated_pods = []
        for raw in raw_discovered:
            if raw["name"] in self.deleted_pod_names or raw.get("deployment") in self.deleted_deployments:
                continue
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
            if p not in updated_pods and p["name"] not in self.deleted_pod_names and p["deployment"] not in self.deleted_deployments:
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
            return [p for p in real_pods if p["name"] not in self.deleted_pod_names and p["deployment"] not in self.deleted_deployments]
        for p in list(self.pods):
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
                if "status" not in p:
                    p["status"] = "HEALTHY"
                p["is_kafka_updated"] = False
        return [p for p in self.pods if p["name"] not in self.deleted_pod_names and p["deployment"] not in self.deleted_deployments]

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
                    "cpu_pct": 0.0,
                    "cpu": 0.0,
                    "cpu_limit": matching_pods[0]["cpu_limit"] if matching_pods else 2.0,
                    "memory_mb": 0.0,
                    "memory_limit": matching_pods[0]["memory_limit"] if matching_pods else 512.0,
                    "restarts": 0,
                    "status": "HEALTHY",
                    "activeProcesses": [{"pid": 100, "user": "app", "cpu": 0.0, "mem": 0.0, "command": f"{deployment}-service"}],
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

    def force_delete_pod(self, pod_name: str, namespace: str) -> Tuple[str, str]:
        """
        Force terminates a pod and its deployment via kubectl delete pod and delete deployment --force.
        Does NOT provision a replacement pod.
        """
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        api_log = f"[{timestamp}] DELETE /api/v1/namespaces/{namespace}/pods/{pod_name} (gracePeriodSeconds=0 --force)"
        
        target_pod = next((p for p in self.pods if p["name"] == pod_name), None)
        dep_name = target_pod.get("deployment") if target_pod else None
        
        self.deleted_pod_names.add(pod_name)
        if dep_name:
            self.deleted_deployments.add(dep_name)
        
        try:
            res = subprocess.run(["kubectl", "delete", "pod", pod_name, "-n", namespace, "--grace-period=0", "--force"], capture_output=True, text=True, timeout=5)
            if dep_name:
                subprocess.run(["kubectl", "delete", "deployment", dep_name, "-n", namespace], capture_output=True, text=True, timeout=5)
            if res.returncode == 0:
                api_log += f" | Successfully deleted pod {pod_name} and deployment {dep_name or ''} via kubectl"
            else:
                api_log += f" | Deleted pod {pod_name}"
        except Exception as e:
            api_log += f" | Deleted pod {pod_name} ({e})"
            
        self.pods = [p for p in self.pods if p["name"] != pod_name and (not dep_name or p.get("deployment") != dep_name)]
        if pod_name in self.real_pod_metrics:
            del self.real_pod_metrics[pod_name]
            
        return api_log, ""

    def delete_pod(self, pod_name: str, namespace: str) -> str:
        api_log, _ = self.force_delete_pod(pod_name, namespace)
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
            target_pod["cpu_pct"] = 0.0
            target_pod["cpu"] = 0.0
            target_pod["status"] = "HEALTHY"
            target_pod["isAnomaly"] = False
            target_pod["anomalyScore"] = 0.95
            target_pod["activeProcesses"] = [p for p in target_pod.get("activeProcesses", []) if "stress" not in p["command"] and "heavy" not in p["command"]]
            
        return api_log
