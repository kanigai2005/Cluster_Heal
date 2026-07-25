import os
import json
import random
import requests
from typing import Dict, List, Tuple
from config import GEMINI_API_KEY, GROQ_API_KEY, RL_LEARNING_RATE, RL_DISCOUNT_FACTOR

ACTIONS = ['scale_up', 'scale_down', 'restart', 'delete_pod', 'kill_process', 'do_nothing']

class QLearningOptimizer:
    def __init__(self):
        # State key format: {deployment}_{cpu_level}_{mem_level}
        # cpu_levels: high_cpu (>90), med_cpu (>70), ok_cpu
        # mem_levels: high_mem (>85), ok_mem
        self.q_table = {}
        self.actions = ACTIONS

    def get_state_key(self, pod: Dict) -> str:
        cpu_pct = pod["cpu_pct"]
        cpu_level = "high_cpu" if cpu_pct > 90 else "med_cpu" if cpu_pct > 70 else "ok_cpu"
        
        mem_pct = (pod["memory_mb"] / pod["memory_limit"]) * 100
        mem_level = "high_mem" if mem_pct > 85 else "ok_mem"
        
        dev = pod["deployment"].lower()
        if "camera" in dev or "traffic" in dev:
            type_key = "traffic"
        elif "water" in dev or "sensor" in dev:
            type_key = "water"
        elif "kitchen" in dev or "smart" in dev:
            type_key = "kitchen"
        else:
            type_key = dev[:12].replace("-", "").replace("_", "")
            
        return f"{type_key}_{cpu_level}_{mem_level}"

    def get_q_values(self, state_key: str) -> Dict[str, float]:
        if state_key not in self.q_table:
            self.q_table[state_key] = {act: 0.0 for act in self.actions}
        return self.q_table[state_key]

    def choose_action(self, state_key: str, epsilon: float, valid_actions: List[str]) -> str:
        """
        Epsilon-greedy policy for action selection.
        """
        if not valid_actions:
            return 'do_nothing'
            
        if random.random() < epsilon:
            return random.choice(valid_actions)
            
        q_vals = self.get_q_values(state_key)
        # Filter for valid actions only
        valid_q = {act: q_vals.get(act, 0.0) for act in valid_actions}
        max_q = max(valid_q.values())
        
        # Handle multiple actions with max Q value
        best_actions = [act for act, val in valid_q.items() if val == max_q]
        return random.choice(best_actions)

    def update_q_value(self, state: str, action: str, reward: float, next_state: str) -> float:
        q_vals = self.get_q_values(state)
        old_q = q_vals[action]
        
        next_q_vals = self.get_q_values(next_state)
        max_next_q = max(next_q_vals.values())
        
        # Bellman update
        new_q = old_q + RL_LEARNING_RATE * (reward + RL_DISCOUNT_FACTOR * max_next_q - old_q)
        q_vals[action] = round(new_q, 2)
        return q_vals[action]


class GroqSREAgent:
    """
    Connects to Groq using REST API (OpenAI-compatible chat completions).
    Provides diagnostic insights, explains root-causes of memory/cpu anomalies,
    and returns precise remedial blueprints.
    """
    def __init__(self):
        self.api_key = GROQ_API_KEY
        self.model = "llama-3.3-70b-versatile"

    def get_recommendations(self, pod: Dict) -> List[Dict]:
        """
        Fetches structured recommendations from Groq.
        If pod is HEALTHY, returns 'no changes required'.
        If pod is UNHEALTHY (CRITICAL, WARNING, STOPPED, ErrImagePull), passes pod state and internal process state to Groq.
        """
        status = pod.get("status", "HEALTHY")
        cpu_pct = pod.get("cpu_pct", 0.0)
        mem_limit = max(1.0, pod.get("memory_limit", 512.0))
        mem_pct = (pod.get("memory_mb", 0.0) / mem_limit) * 100.0
        is_anomaly = pod.get("isAnomaly", False)

        # Check if pod is Healthy
        if status in ["HEALTHY", "Running"] and cpu_pct < 70.0 and mem_pct < 70.0 and not is_anomaly:
            return [
                {
                    "rank": 1,
                    "action": "do_nothing",
                    "reason": f"No changes required. Pod '{pod['name']}' is HEALTHY and running smoothly within normal SLA limits.",
                    "kubectl_command": f"kubectl get pod {pod['name']} -n {pod['namespace']}",
                    "impact": "low"
                }
            ]

        process_str = "\n".join(
            f"PID {p['pid']} [{p['user']}]: CPU {p['cpu']}%, Mem {p['mem']}% -> {p['command']}"
            for p in pod.get("activeProcesses", [])
        )
        if not process_str:
            process_str = "No active internal user processes detected (Pod in " + status + " state)."

        prompt = f"""
        You are an elite Autonomous Site Reliability Engineer (SRE). Analyze the following production pod state alert and internal process list to generate a structured remediation prescription.

        [POD ALERT & STATE DATA]
        Pod Name: {pod['name']}
        Deployment: {pod['deployment']}
        Namespace: {pod['namespace']}
        Current Status / State: {status}
        CPU Current: {cpu_pct:.1f}% of limit ({pod.get('cpu_limit', 2)} Cores)
        Memory Current: {pod.get('memory_mb', 0):.1f} MB of limit ({mem_limit} MB)
        Anomaly Detected: {is_anomaly}

        [INTERNAL CONTAINER PROCESSES (ps aux)]
        {process_str}

        Formulate 1 to 3 technical recommendations based on the pod's status and internal processes.
        Provide the response in raw JSON adhering to this schema:
        An array of objects, each containing:
        {{
          "rank": number (1, 2, or 3),
          "action": string (MUST be one of: "scale_up", "scale_down", "restart", "delete_pod", "kill_process", "do_nothing"),
          "reason": string (a concise, detailed technical explanation analyzing the process/state, max 150 chars),
          "kubectl_command": string (exact kubectl command, e.g. "kubectl scale deployment {pod['deployment']} --replicas=3 -n {pod['namespace']}"),
          "impact": string ("high", "medium", or "low")
        }}
        """

        if not self.api_key:
            return self.get_fallback_recommendations(pod)

        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.api_key}'
            }
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.1,
                "response_format": { "type": "json_object" }
            }
            
            response = requests.post(url, headers=headers, json=payload, timeout=8)
            if response.status_code == 200:
                data = response.json()
                text = data['choices'][0]['message']['content']
                parsed = json.loads(text)
                if isinstance(parsed, dict) and "recommendations" in parsed:
                    return parsed["recommendations"]
                if isinstance(parsed, list):
                    return parsed
        except Exception:
            pass

        return self.get_fallback_recommendations(pod)

    def get_fallback_recommendations(self, pod: Dict) -> List[Dict]:
        dev = pod.get("deployment", "service")
        ns = pod.get("namespace", "default")
        name = pod.get("name", "pod")
        status = pod.get("status", "HEALTHY")
        cpu_pct = pod.get("cpu_pct", 5.0)
        mem_pct = (pod.get("memory_mb", 25.0) / max(1.0, pod.get("memory_limit", 512.0))) * 100.0

        if status in ["HEALTHY", "Running"] and cpu_pct < 70.0 and mem_pct < 70.0:
            return [
                {
                    "rank": 1,
                    "action": "do_nothing",
                    "reason": f"No changes required. Pod '{name}' is HEALTHY and operating normally within SLA thresholds.",
                    "kubectl_command": f"kubectl get pod {name} -n {ns}",
                    "impact": "low"
                }
            ]

        if status == "STOPPED":
            return [
                {
                    "rank": 1,
                    "action": "scale_up",
                    "reason": f"Pod '{name}' is STOPPED. Scale up deployment {dev} to start a new replica.",
                    "kubectl_command": f"kubectl scale deployment {dev} --replicas=1 -n {ns}",
                    "impact": "high"
                },
                {
                    "rank": 2,
                    "action": "restart",
                    "reason": f"Perform rollout restart on deployment {dev} to spin up stopped instances.",
                    "kubectl_command": f"kubectl rollout restart deployment/{dev} -n {ns}",
                    "impact": "high"
                }
            ]

        if status in ["Error in image pulling", "ErrImagePull", "ImagePullBackOff"]:
            return [
                {
                    "rank": 1,
                    "action": "delete_pod",
                    "reason": f"Pod image pull failed ({status}). Delete pod to force image re-fetch or update container spec.",
                    "kubectl_command": f"kubectl delete pod {name} -n {ns} --grace-period=0",
                    "impact": "high"
                },
                {
                    "rank": 2,
                    "action": "restart",
                    "reason": "Rollout restart deployment after verifying container image tag/registry credentials.",
                    "kubectl_command": f"kubectl rollout restart deployment/{dev} -n {ns}",
                    "impact": "medium"
                }
            ]

        if cpu_pct > 70.0:
            return [
                {
                    "rank": 1,
                    "action": "kill_process",
                    "reason": f"Active processes thrashing CPU at {cpu_pct:.1f}%. Terminating CPU stress process restores nominal load.",
                    "kubectl_command": f"kubectl exec -it {name} -n {ns} -- pkill -f stress",
                    "impact": "high"
                },
                {
                    "rank": 2,
                    "action": "scale_up",
                    "reason": f"Scale deployment {dev} to spread high CPU load across replicas.",
                    "kubectl_command": f"kubectl scale deployment {dev} --replicas=3 -n {ns}",
                    "impact": "medium"
                },
                {
                    "rank": 3,
                    "action": "restart",
                    "reason": "Rollout restart deployment to clear thread locks.",
                    "kubectl_command": f"kubectl rollout restart deployment/{dev} -n {ns}",
                    "impact": "medium"
                }
            ]

        return [
            {
                "rank": 1,
                "action": "restart",
                "reason": f"Rollout restart deployment {dev} to clear memory leak and reclaim memory buffers.",
                "kubectl_command": f"kubectl rollout restart deployment/{dev} -n {ns}",
                "impact": "high"
            },
            {
                "rank": 2,
                "action": "scale_down",
                "reason": f"Scale down deployment {dev} if over-provisioned.",
                "kubectl_command": f"kubectl scale deployment {dev} --replicas=1 -n {ns}",
                "impact": "medium"
            }
        ]

GeminiSREAgent = GroqSREAgent
