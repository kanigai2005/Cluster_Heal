import os
import json
import random
import requests
from typing import Dict, List, Tuple
from config import GEMINI_API_KEY, RL_LEARNING_RATE, RL_DISCOUNT_FACTOR

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


class GeminiSREAgent:
    """
    Connects to Gemini-2.5-Flash using REST API.
    Provides diagnostic insights, explains root-causes of memory/cpu anomalies,
    and returns precise remedial blueprints.
    """
    def __init__(self):
        self.api_key = GEMINI_API_KEY
        self.model = "gemini-2.5-flash"

    def get_recommendations(self, pod: Dict) -> List[Dict]:
        """
        Fetches structured recommendations from Gemini 2.5 Flash.
        If API Key is missing or request fails, falls back gracefully to a robust rule-based advisor.
        """
        process_str = "\n".join(
            f"PID {p['pid']} [{p['user']}]: CPU {p['cpu']}%, Mem {p['mem']}% -> {p['command']}"
            for p in pod.get("activeProcesses", [])
        )
        
        prompt = f"""
        You are an elite Autonomous Site Reliability Engineer (SRE). Analyze the following production metric alert and container process list to generate a structured remediation prescription.

        [ALERT DATA]
        Pod: {pod['name']}
        Deployment: {pod['deployment']}
        Namespace: {pod['namespace']}
        CPU Current: {pod['cpu_pct']:.1f}% of limit ({pod['cpu_limit']} Cores)
        Memory Current: {pod['memory_mb']:.1f} MB of limit ({pod['memory_limit']} MB)

        [PROCESSES (ps aux)]
        {process_str}

        Formulate EXACTLY THREE technical recommendations to resolve this load or memory footprint.
        Provide the response in raw JSON adhering to this schema:
        An array of exactly three objects, each containing:
        {{
          "rank": number (1, 2, or 3),
          "action": string (MUST be one of: "scale_up", "scale_down", "restart", "delete_pod", "kill_process", "do_nothing"),
          "reason": string (a concise, detailed technical explanation of why this action is correct, maximum 150 characters),
          "kubectl_command": string (the exact kubectl command the user would run, e.g. "kubectl scale deployment esp-traffic-camera --replicas=3 -n town-traffic"),
          "impact": string (MUST be "high", "medium", or "low")
        }}
        """

        if not self.api_key:
            return self.get_fallback_recommendations(pod)

        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}"
            headers = {'Content-Type': 'application/json'}
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "responseMimeType": "application/json",
                    "responseSchema": {
                        "type": "ARRAY",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "rank": {"type": "INTEGER"},
                                "action": {"type": "STRING"},
                                "reason": {"type": "STRING"},
                                "kubectl_command": {"type": "STRING"},
                                "impact": {"type": "STRING"}
                            },
                            "required": ["rank", "action", "reason", "kubectl_command", "impact"]
                        }
                    }
                }
            }
            
            response = requests.post(url, headers=headers, json=payload, timeout=8)
            if response.status_code == 200:
                data = response.json()
                text = data['candidates'][0]['content']['parts'][0]['text']
                return json.loads(text)
        except Exception:
            pass

        return self.get_fallback_recommendations(pod)

    def get_fallback_recommendations(self, pod: Dict) -> List[Dict]:
        dev = pod["deployment"]
        ns = pod["namespace"]
        name = pod["name"]
        
        if "traffic-camera" in dev or "camera" in dev.lower() or "traffic" in dev.lower():
            return [
                {
                    "rank": 1,
                    "action": "scale_up",
                    "reason": "High camera feed ingestion exceeds single-instance limits. Distributing traffic resolves thread locks.",
                    "kubectl_command": f"kubectl scale deployment {dev} --replicas=3 -n {ns}",
                    "impact": "high"
                },
                {
                    "rank": 2,
                    "action": "restart",
                    "reason": "Graceful pod rolling restart clears active video pipeline memory allocation buffers.",
                    "kubectl_command": f"kubectl rollout restart deployment/{dev} -n {ns}",
                    "impact": "medium"
                },
                {
                    "rank": 3,
                    "action": "do_nothing",
                    "reason": "Let cluster autoscaler execute generic scaling protocols.",
                    "kubectl_command": "echo 'SRE standby'",
                    "impact": "low"
                }
            ]
        elif "smart-kitchen" in dev or "kitchen" in dev.lower() or "smart" in dev.lower():
            return [
                {
                    "rank": 1,
                    "action": "restart",
                    "reason": "JVM garbage collection is thrashing due to continuous heap leaks. Hard restart flushes state.",
                    "kubectl_command": f"kubectl rollout restart deployment/{dev} -n {ns}",
                    "impact": "high"
                },
                {
                    "rank": 2,
                    "action": "delete_pod",
                    "reason": "Forcefully terminating the pod triggers immediate scheduler placement on a healthier node.",
                    "kubectl_command": f"kubectl delete pod {name} -n {ns} --grace-period=0",
                    "impact": "high"
                },
                {
                    "rank": 3,
                    "action": "scale_up",
                    "reason": "Horizontal scaling acts as a buffer but won't resolve underlying code leaks long-term.",
                    "kubectl_command": f"kubectl scale deployment {dev} --replicas=3 -n {ns}",
                    "impact": "low"
                }
            ]
        else:
            cpu_pct = pod.get("cpu_pct", 5.0)
            mem_pct = (pod.get("memory_mb", 25.0) / max(1.0, pod.get("memory_limit", 512.0))) * 100.0
            
            if cpu_pct > 70.0:
                return [
                    {
                        "rank": 1,
                        "action": "kill_process",
                        "reason": f"Active process threads are thrashing CPU at {cpu_pct:.1f}%. Hard-terminating stressor restores nominal state.",
                        "kubectl_command": f"kubectl exec -it {name} -n {ns} -- pkill -f stress || kill -9 1",
                        "impact": "high"
                    },
                    {
                        "rank": 2,
                        "action": "scale_up",
                        "reason": f"Scale out {dev} horizontally to spread CPU bottlenecks across multiple replicas.",
                        "kubectl_command": f"kubectl scale deployment {dev} --replicas=3 -n {ns}",
                        "impact": "medium"
                    },
                    {
                        "rank": 3,
                        "action": "restart",
                        "reason": "Perform rollout restart to cycle active sockets and thread locks.",
                        "kubectl_command": f"kubectl rollout restart deployment/{dev} -n {ns}",
                        "impact": "medium"
                    }
                ]
            else:
                return [
                    {
                        "rank": 1,
                        "action": "restart",
                        "reason": f"Rollout restart {dev} to clean active memory leaks and free system file descriptors.",
                        "kubectl_command": f"kubectl rollout restart deployment/{dev} -n {ns}",
                        "impact": "high"
                    },
                    {
                        "rank": 2,
                        "action": "delete_pod",
                        "reason": f"Delete pod {name} to force Kubernetes to instantly spin up a replacement node.",
                        "kubectl_command": f"kubectl delete pod {name} -n {ns} --grace-period=0",
                        "impact": "high"
                    },
                    {
                        "rank": 3,
                        "action": "do_nothing",
                        "reason": "Nominal load levels. Standby and continue telemetry stream scans.",
                        "kubectl_command": "echo 'SRE nominal state'",
                        "impact": "low"
                    }
                ]
