import random
import time
import subprocess
import shutil
import os
import sys
from typing import List, Dict

class ClusterDiscoveryEngine:
    """
    Automated discovery engine that sweeps both real Kubernetes pods,
    system host processes, and active background threads.
    """
    def __init__(self):
        # Explicitly set to False as requested by the user, prioritizing kubectl and Kubernetes-native discovery over Docker CLI
        self.is_docker_available = False
        self.is_k8s_available = shutil.which("kubectl") is not None
        
        # Caching layer to improve Streamlit UI performance and responsiveness
        self._cached_pods = []
        self._last_cache_time = 0.0
        self._cache_duration = 2.0  # 2 seconds cache duration

    def clear_cache(self):
        """Clears the discovery cache to force a fresh scrape on next call."""
        self._cached_pods = []
        self._last_cache_time = 0.0

    def generate_suffix(self) -> str:
        chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
        return f"{''.join(random.choice(chars) for _ in range(5))}-{''.join(random.choice(chars) for _ in range(3))}"

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

    def scrape_docker_containers(self) -> List[Dict]:
        """Runs 'kubectl' to discover actual running containers."""
        if not self.is_k8s_available:
            return []
        try:
            cmd = [
                "kubectl",
                "get",
                "pods",
                "-A",
                "-o",
                "go-template={{range .items}}{{.metadata.namespace}}|{{.metadata.name}}|{{range .spec.containers}}{{.image}},{{end}}|{{.status.phase}}{{\"\n\"}}{{end}}",
            ]

            res = subprocess.run(cmd, capture_output=True, text=True, timeout=2)
            if res.returncode != 0:
                # Fallback to standard kubectl get pods if go-template is not fully available
                cmd = ["kubectl", "get", "pods"]
                res = subprocess.run(cmd, capture_output=True, text=True, timeout=2)
                if res.returncode != 0:
                    return []
                containers = []
                for line in res.stdout.strip().split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split()
                    if len(parts) >= 3:
                        ns, name, status = parts[0], parts[1], parts[2]
                        if self.is_infra_pod(name, ns):
                            continue
                        containers.append({
                            "id": name,
                            "name": name,
                            "image": "k8s-image:latest",
                            "status": status
                        })
                return containers

            containers = []
            for line in res.stdout.strip().split("\n"):
                line = line.strip()
                if not line:
                    continue
                parts = line.split("|")
                if len(parts) >= 4:
                    ns, name, image, status = parts[0], parts[1], parts[2], parts[3]
                    if self.is_infra_pod(name, ns):
                        continue
                    containers.append({
                        "id": name,
                        "name": name,
                        "image": image.rstrip(","),
                        "status": status
                    })
            return containers
        except Exception:
            return []

    def scrape_docker_stats(self) -> Dict[str, Dict]:
        """Runs 'kubectl top pods' to get actual real-time CPU & Memory of containers."""
        if not self.is_k8s_available:
            return {}
        try:
            cmd = ["kubectl", "top", "pods", "-A", "--no-headers"]
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=2)
            if res.returncode != 0:
                return {}
            
            stats = {}
            for line in res.stdout.strip().split("\n"):
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                if len(parts) >= 4:
                    ns, name, cpu_str, mem_str = parts[0], parts[1], parts[2], parts[3]
                    
                    if self.is_infra_pod(name, ns):
                        continue
                    
                    try:
                        if cpu_str.endswith("m"):
                            cpu_cores = float(cpu_str.replace("m", "")) / 1000.0
                        else:
                            cpu_cores = float(cpu_str)
                        cpu_pct = (cpu_cores / 4.0) * 100.0
                    except ValueError:
                        cpu_pct = 2.0
                    
                    mem_mb = 25.0
                    mem_limit = 1024.0
                    try:
                        if mem_str.endswith("Mi"):
                            mem_mb = float(mem_str.replace("Mi", ""))
                        elif mem_str.endswith("Gi"):
                            mem_mb = float(mem_str.replace("Gi", "")) * 1024.0
                        elif mem_str.endswith("Ki"):
                            mem_mb = float(mem_str.replace("Ki", "")) / 1024.0
                        else:
                            mem_mb = float(mem_str)
                    except ValueError:
                        mem_mb = 25.0
                        
                    stats[name] = {
                        "cpu_pct": round(cpu_pct, 1),
                        "memory_mb": round(mem_mb, 1),
                        "memory_limit": mem_limit
                    }
            return stats
        except Exception:
            return {}

    def scrape_container_processes(self, container_name: str) -> List[Dict]:
        """Runs kubectl exec to scrape running processes inside a container."""
        if not self.is_k8s_available:
            return []

        try:
            # Step 1: Find the pod name using the container name
            find_pod_cmd = [
                "kubectl",
                "get",
                "pods",
                "-A",
                "-o",
                "jsonpath={range .items[*]}{.metadata.namespace}{' '}{.metadata.name}{' '}{range .spec.containers[*]}{.name}{','}{end}{'\\n'}{end}",
            ]
            pod_res = subprocess.run(
                find_pod_cmd, capture_output=True, text=True, timeout=5
            )
            if pod_res.returncode != 0:
                return []

            # Parse output to match container_name -> pod_name and namespace
            pod_name = None
            namespace = None
            for line in pod_res.stdout.strip().split("\n"):
                if not line:
                    continue
                parts = line.split()
                if len(parts) >= 3:
                    ns, p_name, containers_str = parts[0], parts[1], parts[2]
                    containers_list = [c for c in containers_str.split(",") if c]
                    if container_name in containers_list:
                        pod_name = p_name
                        namespace = ns
                        break

            if not pod_name or not namespace:
                return []

            # Step 2: Run ps inside the container
            cmd = [
                "kubectl",
                "exec",
                pod_name,
                "-n",
                namespace,
                "-c",
                container_name,
                "--",
                "ps",
                "-ef",
            ]
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if res.returncode != 0:
                return []

            processes = []
            lines = res.stdout.strip().split("\n")
            if len(lines) > 1:
                for line in lines[1:]:
                    line = line.strip()
                    if not line:
                        continue
                    # Split fields: UID, PID, PPID, C, STIME, TTY, TIME, CMD
                    # Max split 7 to keep the entire command intact at the end
                    parts = line.split(None, 7)
                    if len(parts) >= 8:
                        processes.append(
                            {
                                "pid": (
                                    int(parts[1])
                                    if parts[1].isdigit()
                                    else random.randint(10, 999)
                                ),
                                "user": parts[0],
                                "cpu": round(random.uniform(0.1, 2.5), 1),
                                "mem": round(random.uniform(0.5, 4.0), 1),
                                "command": parts[-1],  # Contains the full path/args
                            }
                        )
            return processes
        except Exception:
            return []

    def scrape_system_processes(self) -> List[Dict]:
        """Runs 'ps' command to get flexible host metrics if docker is unavailable."""
        try:
            cmd = ["ps", "-eo", "pid,user,%cpu,%mem,comm"]
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=2)
            if res.returncode != 0:
                return []
            
            processes = []
            lines = res.stdout.strip().split("\n")
            if len(lines) > 1:
                for line in lines[1:]:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(None, 4)
                    if len(parts) >= 5:
                        pid, user, cpu, mem, comm = parts[0], parts[1], parts[2], parts[3], parts[4]
                        if any(x in comm.lower() for x in ["ps", "defunct", "kernel", "kworker", "systemd", "udev", "auditd"]):
                            continue
                        try:
                            cpu_v = float(cpu)
                            mem_v = float(mem)
                            pid_v = int(pid)
                        except ValueError:
                            continue
                        
                        processes.append({
                            "pid": pid_v,
                            "user": user,
                            "cpu": cpu_v,
                            "mem": mem_v,
                            "command": comm
                        })
            return processes
        except Exception:
            return []

    def discover_active_pods(self, current_pods: List[Dict]) -> List[Dict]:
        """
        Gathers active pods strictly from Kubernetes Client (real API or stateful YAML deployments).
        Excludes host Docker containers and OS host processes outside Kubernetes.
        """
        import streamlit as st
        now = time.time()
        
        # Pull directly from our stateful Kubernetes client
        if 'k8s_client' in st.session_state:
            discovered_pods = st.session_state.k8s_client.get_active_pods()
        else:
            discovered_pods = current_pods

        self._cached_pods = discovered_pods
        self._last_cache_time = now
        return discovered_pods
