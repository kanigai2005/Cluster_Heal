import time
import random
import os
import sys
import subprocess
from typing import Dict, List, Optional

class KubernetesClientSimulator:
    """
    High-fidelity stateful controller simulating Kubernetes API Client.
    Provides actual functional orchestration for horizontal scaling, pod deletions, rollout restarts,
    and process level container execution inside the SRE sandbox env.
    """
    def __init__(self):
        self.deployment_replicas = {}
        self.pods = []
        self.spawned_pids = {}       # pod_name -> pid
        self.spawned_processes = {}  # pod_name -> subprocess.Popen

    def generate_suffix(self) -> str:
        chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
        return f"{''.join(random.choice(chars) for _ in range(5))}-{''.join(random.choice(chars) for _ in range(3))}"

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
        for p in self.pods:
            p["replicas"] = self.deployment_replicas.get(p["deployment"], p.get("replicas", 1))
        return list(self.pods)

    def scale_deployment(self, deployment: str, replicas: int, namespace: str) -> str:
        """
        Actually scales deployment replicas, spawning real background processes/containers or terminating them.
        """
        old_replicas = self.deployment_replicas.get(deployment, 1)
        self.deployment_replicas[deployment] = replicas
        
        dep_pods = [p for p in self.pods if p["deployment"] == deployment]
        current_count = len(dep_pods)
        
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        api_log = f"[{timestamp}] PATCH /apis/apps/v1/namespaces/{namespace}/deployments/{deployment}/scale (replicas={replicas})"
        
        diff = replicas - current_count
        if diff > 0:
            spawned = []
            for _ in range(diff):
                p_name = f"{deployment}-{self.generate_suffix()}"
                
                parent_pod = dep_pods[0] if dep_pods else None
                is_docker = parent_pod and parent_pod.get("type") == "docker"
                
                new_pid = None
                new_container_id = None
                
                if is_docker and parent_pod:
                    try:
                        image = parent_pod.get("deployment") + ":latest"
                        res = subprocess.run(["docker", "run", "-d", image], capture_output=True, text=True, timeout=3)
                        if res.returncode == 0:
                            new_container_id = res.stdout.strip()
                    except Exception:
                        pass
                else:
                    try:
                        p_obj = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(1200)"])
                        new_pid = p_obj.pid
                        self.spawned_processes[p_name] = p_obj
                        self.spawned_pids[p_name] = new_pid
                    except Exception:
                        pass
                
                new_pod = {
                    "name": p_name,
                    "namespace": namespace,
                    "deployment": deployment,
                    "cpu": 0.05,
                    "cpu_pct": 2.0,
                    "cpu_limit": parent_pod["cpu_limit"] if parent_pod else 4.0,
                    "memory_mb": parent_pod["memory_mb"] if parent_pod else 45.0,
                    "memory_limit": parent_pod["memory_limit"] if parent_pod else 1024.0,
                    "restarts": 0,
                    "status": "HEALTHY",
                    "activeProcesses": [{"pid": new_pid or random.randint(1000, 9999), "user": "sre-agent", "cpu": 0.1, "mem": 0.5, "command": "python -c 'replica_task'"}],
                    "creationTime": int(time.time() * 1000),
                    "replicas": replicas,
                    "isAnomaly": False,
                    "anomalyScore": 0.95,
                    "history": [],
                    "is_real": True,
                    "type": "docker" if is_docker else "process",
                    "id": new_container_id,
                    "pid": new_pid
                }
                self.pods.append(new_pod)
                spawned.append(p_name)
                
            api_log += f" | ReplicaSetController scaled UP (+{diff} pods: {', '.join(spawned)})"
        elif diff < 0:
            removed = []
            for _ in range(abs(diff)):
                p_to_remove = next((p for p in reversed(self.pods) if p["deployment"] == deployment), None)
                if p_to_remove:
                    removed.append(p_to_remove["name"])
                    
                    if p_to_remove.get("type") == "docker" and p_to_remove.get("id"):
                        try:
                            subprocess.run(["docker", "rm", "-f", p_to_remove["id"]], capture_output=True, timeout=2)
                        except Exception:
                            pass
                    elif p_to_remove.get("pid"):
                        pid = p_to_remove["pid"]
                        try:
                            os.kill(pid, 9)
                        except Exception:
                            pass
                        self.spawned_processes.pop(p_to_remove["name"], None)
                        self.spawned_pids.pop(p_to_remove["name"], None)
                        
                    self.pods.remove(p_to_remove)
            api_log += f" | ReplicaSetController scaled DOWN (-{abs(diff)} pods: {', '.join(removed)})"
        else:
            api_log += " | ReplicaSetController state aligned: no action required."
            
        return api_log

    def delete_pod(self, pod_name: str, namespace: str) -> str:
        """
        Actually terminates a container or process, then triggers scheduler placement to start a fresh replacement.
        """
        target_pod = next((p for p in self.pods if p["name"] == pod_name), None)
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        if not target_pod:
            return f"[{timestamp}] DELETE /api/v1/namespaces/{namespace}/pods/{pod_name} failed: Pod not found"
            
        deployment = target_pod["deployment"]
        desired_replicas = self.deployment_replicas.get(deployment, 1)
        is_docker = target_pod.get("type") == "docker"
        
        # Kill the real process or container
        if is_docker and target_pod.get("id"):
            try:
                subprocess.run(["docker", "rm", "-f", target_pod["id"]], capture_output=True, timeout=2)
            except Exception:
                pass
        elif target_pod.get("pid"):
            pid = target_pod["pid"]
            if pid != os.getpid():
                try:
                    os.kill(pid, 9)
                except Exception:
                    pass
            self.spawned_processes.pop(pod_name, None)
            self.spawned_pids.pop(pod_name, None)
            
        # Delete from tracking state
        self.pods.remove(target_pod)
        api_log = f"[{timestamp}] DELETE /api/v1/namespaces/{namespace}/pods/{pod_name} (gracePeriodSeconds=0)"
        
        # Check if replacement is needed to sustain desired replicas
        dep_pods = [p for p in self.pods if p["deployment"] == deployment]
        if len(dep_pods) < desired_replicas:
            new_name = f"{deployment}-{self.generate_suffix()}"
            new_pid = None
            new_container_id = None
            
            if is_docker:
                try:
                    res = subprocess.run(["docker", "run", "-d", deployment + ":latest"], capture_output=True, text=True, timeout=3)
                    if res.returncode == 0:
                        new_container_id = res.stdout.strip()
                except Exception:
                    pass
            else:
                try:
                    p_obj = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(1200)"])
                    new_pid = p_obj.pid
                    self.spawned_processes[new_name] = p_obj
                    self.spawned_pids[new_name] = new_pid
                except Exception:
                    pass
                    
            replacement = {
                "name": new_name,
                "namespace": namespace,
                "deployment": deployment,
                "cpu": 0.05,
                "cpu_pct": 2.0,
                "cpu_limit": target_pod.get("cpu_limit", 4.0),
                "memory_mb": target_pod.get("memory_mb", 45.0),
                "memory_limit": target_pod.get("memory_limit", 1024.0),
                "restarts": target_pod.get("restarts", 0) + 1,
                "status": "HEALTHY",
                "activeProcesses": [{"pid": new_pid or random.randint(1000, 9999), "user": "sre-agent", "cpu": 0.1, "mem": 0.5, "command": "python -c 'replica_task'"}],
                "creationTime": int(time.time() * 1000),
                "replicas": desired_replicas,
                "isAnomaly": False,
                "anomalyScore": 0.95,
                "history": [],
                "is_real": True,
                "type": "docker" if is_docker else "process",
                "id": new_container_id,
                "pid": new_pid
            }
            self.pods.append(replacement)
            api_log += f" | Scheduler detected replacement requirement. Created pod {replacement['name']}"
            
        return api_log

    def rollout_restart(self, deployment: str, namespace: str) -> str:
        """
        Performs rollout restart by terminating all pods of the deployment, clearing stress loads, and restarting them.
        """
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        api_log = f"[{timestamp}] POST /apis/apps/v1/namespaces/{namespace}/deployments/{deployment}/rollout/restart"
        
        desired_replicas = self.deployment_replicas.get(deployment, 1)
        dep_pods = [p for p in self.pods if p["deployment"] == deployment]
        old_names = [p["name"] for p in dep_pods]
        
        # Kill & remove all old ones
        for p in dep_pods:
            if p.get("type") == "docker" and p.get("id"):
                try:
                    subprocess.run(["docker", "rm", "-f", p["id"]], capture_output=True, timeout=2)
                except Exception:
                    pass
            elif p.get("pid"):
                pid = p["pid"]
                if pid != os.getpid():
                    try:
                        os.kill(pid, 9)
                    except Exception:
                        pass
                self.spawned_processes.pop(p["name"], None)
                self.spawned_pids.pop(p["name"], None)
                
            if p in self.pods:
                self.pods.remove(p)
                
        # Re-create fresh ones to match replica target
        new_names = []
        for _ in range(desired_replicas):
            p_name = f"{deployment}-{self.generate_suffix()}"
            new_pid = None
            new_container_id = None
            
            is_docker = dep_pods and dep_pods[0].get("type") == "docker"
            if is_docker:
                try:
                    res = subprocess.run(["docker", "run", "-d", deployment + ":latest"], capture_output=True, text=True, timeout=3)
                    if res.returncode == 0:
                        new_container_id = res.stdout.strip()
                except Exception:
                    pass
            else:
                try:
                    p_obj = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(1200)"])
                    new_pid = p_obj.pid
                    self.spawned_processes[p_name] = p_obj
                    self.spawned_pids[p_name] = new_pid
                except Exception:
                    pass
            
            replacement = {
                "name": p_name,
                "namespace": namespace,
                "deployment": deployment,
                "cpu": 0.05,
                "cpu_pct": 2.0,
                "cpu_limit": dep_pods[0].get("cpu_limit", 4.0) if dep_pods else 4.0,
                "memory_mb": dep_pods[0].get("memory_mb", 45.0) if dep_pods else 45.0,
                "memory_limit": dep_pods[0].get("memory_limit", 1024.0) if dep_pods else 1024.0,
                "restarts": (dep_pods[0].get("restarts", 0) if dep_pods else 0) + 1,
                "status": "HEALTHY",
                "activeProcesses": [{"pid": new_pid or random.randint(1000, 9999), "user": "sre-agent", "cpu": 0.1, "mem": 0.5, "command": "python -c 'replica_task'"}],
                "creationTime": int(time.time() * 1000),
                "replicas": desired_replicas,
                "isAnomaly": False,
                "anomalyScore": 0.95,
                "history": [],
                "is_real": True,
                "type": "docker" if is_docker else "process",
                "id": new_container_id,
                "pid": new_pid
            }
            self.pods.append(replacement)
            new_names.append(p_name)
            
        api_log += f" | RollingUpgrade: Terminated ({', '.join(old_names)}) -> Created ({', '.join(new_names)})"
        return api_log

    def exec_container_kill(self, pod_name: str, namespace: str, container: str, process_name: str) -> str:
        """
        Executes terminal process pkill / kill inside the target pod, clearing stress loads and resolving anomalies.
        """
        target_pod = next((p for p in self.pods if p["name"] == pod_name), None)
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        if not target_pod:
            return f"[{timestamp}] POST /api/v1/namespaces/{namespace}/pods/{pod_name}/exec failed: Pod not found"
            
        api_log = f"[{timestamp}] POST /api/v1/namespaces/{namespace}/pods/{pod_name}/exec?command=killall&args={process_name}"
        
        target_pod["is_stressed"] = False
        target_pod["cpu_pct"] = random.uniform(3.5, 6.2)
        target_pod["status"] = "HEALTHY"
        target_pod["isAnomaly"] = False
        target_pod["anomalyScore"] = 0.95
        
        target_pod["activeProcesses"] = [p for p in target_pod.get("activeProcesses", []) if "stress" not in p["command"] and "heavy" not in p["command"]]
        
        is_docker = target_pod.get("type") == "docker"
        if is_docker and target_pod.get("id"):
            try:
                subprocess.run(["docker", "exec", target_pod["id"], "pkill", "-f", process_name], capture_output=True, timeout=2)
            except Exception:
                pass
        else:
            try:
                subprocess.run(["pkill", "-f", process_name], capture_output=True, timeout=2)
            except Exception:
                pass
                
        api_log += f" | Terminated target processes of name '{process_name}' inside '{container or 'container'}'. CPU load normalized."
        return api_log
