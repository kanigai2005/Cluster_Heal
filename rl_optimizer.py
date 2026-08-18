import os
import json
import random
import requests
import sqlite3
from typing import Dict, List, Tuple
from config import GROQ_API_KEY, RL_LEARNING_RATE, RL_DISCOUNT_FACTOR

ACTIONS = ['force_delete', 'restart', 'scale_up', 'scale_down', 'do_nothing']

class QLearningOptimizer:
    def __init__(self, db_path: str = "sre_agent.db"):
        # State key format: {deployment}_{cpu_level}_{mem_level}
        # cpu_levels: high_cpu (>90), med_cpu (>70), ok_cpu
        # mem_levels: high_mem (>85), ok_mem
        self.q_table = {}
        self.actions = ACTIONS
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS q_table (
                state_key TEXT,
                action TEXT,
                q_value REAL,
                PRIMARY KEY (state_key, action)
            )
        ''')
        self.conn.commit()
        self._load_q_table()

    def _load_q_table(self):
        self.cursor.execute("SELECT state_key, action, q_value FROM q_table")
        rows = self.cursor.fetchall()
        for state_key, action, q_value in rows:
            if state_key not in self.q_table:
                self.q_table[state_key] = {act: 0.0 for act in self.actions}
            self.q_table[state_key][action] = q_value

    def reset_q_table(self):
        """Clears the Q-table in memory and in the SQLite database."""
        self.q_table = {}
        self.cursor.execute("DELETE FROM q_table")
        self.conn.commit()

    def get_state_key(self, pod: Dict) -> str:
        cpu_pct = pod.get("cpu_pct", 0.0)
        cpu_level = "high_cpu" if cpu_pct > 90 else "med_cpu" if cpu_pct > 70 else "ok_cpu"
        
        mem_limit = max(1.0, pod.get("memory_limit", 512.0))
        mem_pct = (pod.get("memory_mb", 0.0) / mem_limit) * 100
        mem_level = "high_mem" if mem_pct > 85 else "ok_mem"
        
        dev = pod.get("deployment", "service").lower()
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
            return 'restart'
            
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
        if action not in q_vals:
            q_vals[action] = 0.0
        old_q = q_vals[action]
        
        next_q_vals = self.get_q_values(next_state)
        max_next_q = max(next_q_vals.values()) if next_q_vals else 0.0
        
        # Bellman update
        new_q = old_q + RL_LEARNING_RATE * (reward + RL_DISCOUNT_FACTOR * max_next_q - old_q)
        q_vals[action] = round(new_q, 2)
        
        # Save to SQLite
        self.cursor.execute('''
            INSERT OR REPLACE INTO q_table (state_key, action, q_value) 
            VALUES (?, ?, ?)
        ''', (state, action, q_vals[action]))
        self.conn.commit()
        
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
        Passes pod state and internal process state to Groq.
        """
        status = pod.get("status", "HEALTHY")
        cpu_pct = pod.get("cpu_pct", 0.0)
        mem_limit = max(1.0, pod.get("memory_limit", 512.0))
        mem_pct = (pod.get("memory_mb", 0.0) / mem_limit) * 100.0
        is_anomaly = pod.get("isAnomaly", False)

        process_str = "\n".join(
            f"PID {p['pid']} [{p['user']}]: CPU {p['cpu']}%, Mem {p['mem']}% -> {p['command']}"
            for p in pod.get("activeProcesses", [])
        )
        if not process_str:
            process_str = "No active internal user processes detected (Pod in " + status + " state)."

        # Health check: if the pod is working fine, avoid calling Groq
        if status in ["HEALTHY", "Running", "OK"] and cpu_pct < 70.0 and mem_pct < 80.0 and not is_anomaly:
            # Check for suspicious processes like 'stress' or 'leak'
            has_suspicious_process = any(
                "stress" in p.get("command", "").lower() or "leak" in p.get("command", "").lower()
                for p in pod.get("activeProcesses", [])
            )
            if not has_suspicious_process:
                return [{
                    "rank": 1,
                    "action": "do_nothing",
                    "reason": "Pod is healthy and operating within normal parameters. No remediation required.",
                    "kubectl_command": "N/A",
                    "impact": "low"
                }]

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

        refer Q table for past analysis choose the best action
        {self.q_table}

        Formulate 1 to 3 technical recommendations based on the pod's status and internal processes.
        Provide the response in raw JSON adhering to this schema:
        An array of objects, each containing:
        {{
          "rank": number (1, 2, or 3),
          "action": string (MUST be one of: "force_delete", "restart", "scale_up", "scale_down","deleting or restarting any other process which is causing an issue inside the container"),
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

        if status in ["STOPPED", "Error in image pulling", "ErrImagePull", "ImagePullBackOff"]:
            return [
                {
                    "rank": 1,
                    "action": "force_delete",
                    "reason": f"Pod '{name}' is in non-ready state ({status}). Force delete to replace with clean instance.",
                    "kubectl_command": f"kubectl delete pod {name} -n {ns} --grace-period=0 --force",
                    "impact": "high"
                },
                {
                    "rank": 2,
                    "action": "restart",
                    "reason": f"Perform rollout restart on deployment {dev}.",
                    "kubectl_command": f"kubectl rollout restart deployment/{dev} -n {ns}",
                    "impact": "medium"
                }
            ]

        if cpu_pct > 70.0:
            return [
                {
                    "rank": 1,
                    "action": "scale_up",
                    "reason": f"Scale deployment {dev} to spread high CPU load ({cpu_pct:.1f}%) across replicas.",
                    "kubectl_command": f"kubectl scale deployment {dev} --replicas=3 -n {ns}",
                    "impact": "high"
                },
                {
                    "rank": 2,
                    "action": "force_delete",
                    "reason": f"Force delete pod '{name}' to force fresh container allocation.",
                    "kubectl_command": f"kubectl delete pod {name} -n {ns} --grace-period=0 --force",
                    "impact": "high"
                },
                {
                    "rank": 3,
                    "action": "restart",
                    "reason": "Rollout restart deployment to clear process threads.",
                    "kubectl_command": f"kubectl rollout restart deployment/{dev} -n {ns}",
                    "impact": "medium"
                }
            ]

        return [
            {
                "rank": 1,
                "action": "restart",
                "reason": f"Rollout restart deployment {dev} to recycle container memory buffers.",
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

    def rank_recommendations(self, pod: Dict, recs: List[Dict], rl_optimizer) -> List[Dict]:
        """
        Ranks recommendations based on the individual pod's telemetry state AND Q-table scores.
        """
        state_key = rl_optimizer.get_state_key(pod)
        q_vals = rl_optimizer.get_q_values(state_key)
        
        enriched = []
        for r in recs:
            act = r.get("action", "restart")
            q_act = "force_delete" if act in ["force_delete", "delete_pod"] else act
            q_score = q_vals.get(q_act, 0.0)
            
            item = dict(r)
            item["q_score"] = q_score
            item["state_key"] = state_key
            enriched.append(item)
            
        # Sort descending by Q-score
        enriched.sort(key=lambda x: x["q_score"], reverse=True)
        
        for idx, item in enumerate(enriched):
            item["rank"] = idx + 1
            
        return enriched

