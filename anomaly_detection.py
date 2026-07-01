import random
import time
from typing import Tuple, List, Dict
import numpy as np

try:
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

class SREAnomalyDetector:
    def __init__(self):
        self.is_active = SKLEARN_AVAILABLE
        # Seed some baseline normal training data (CPU ~10-40%, MEM ~15-45%)
        if self.is_active:
            np.random.seed(42)
            normal_cpu = np.random.normal(25.0, 8.0, 100)
            normal_mem = np.random.normal(30.0, 10.0, 100)
            self.training_data = np.column_stack((normal_cpu, normal_mem))
            
            self.model = IsolationForest(n_estimators=50, contamination=0.1, random_state=42)
            self.model.fit(self.training_data)
        else:
            self.training_data = []
            self.model = None

    def analyze_metrics(self, cpu_pct: float, mem_pct: float) -> Tuple[bool, float]:
        """
        Analyzes a pod's metrics and returns (is_anomaly: bool, anomaly_score: float)
        IsolationForest returns negative scores for anomalies (closer to -1.0) and positive for normal.
        """
        if self.is_active and self.model is not None:
            try:
                X = np.array([[cpu_pct, mem_pct]])
                # decision_function yields scores. Lower score means more anomalous.
                score = float(self.model.decision_function(X)[0])
                # Isolation Forest standard threshold is around 0.0 or slightly negative
                is_anomaly = score < -0.15
                return is_anomaly, round(score, 3)
            except Exception:
                pass
        
        # Robust fallback mathematical model if sklearn is missing or fails
        # Normal behavior is CPU < 70% and Memory < 70%
        # Anomalous behavior scales exponentially as it approaches limits
        cpu_dev = max(0.0, cpu_pct - 50.0) / 50.0
        mem_dev = max(0.0, mem_pct - 50.0) / 50.0
        
        # Calculate a pseudo anomaly score between -1.0 and 1.0
        # High CPU/MEM yields lower score (more anomalous)
        severity = (cpu_dev ** 2) + (mem_dev ** 2)
        score = 0.5 - min(1.5, severity)
        
        is_anomaly = score < -0.2
        return is_anomaly, round(score, 3)

    def generate_alerts(self, pods: List[Dict]) -> List[Dict]:
        """
        Processes active pods and yields a list of high-fidelity alerts
        """
        alerts = []
        for p in pods:
            mem_pct = (p["memory_mb"] / p["memory_limit"]) * 100.0
            is_anomaly, score = self.analyze_metrics(p["cpu_pct"], mem_pct)
            
            if is_anomaly:
                metric_type = "CPU" if p["cpu_pct"] > mem_pct else "MEMORY"
                alerts.append({
                    "pod_name": p["name"],
                    "deployment": p["deployment"],
                    "metric_type": metric_type,
                    "anomaly_score": score,
                    "timestamp_ms": int(time.time() * 1000),
                    "details": f"Anomaly detected in {p['deployment']} with {metric_type} exceeding normal operating limits. Score: {score}."
                })
        return alerts
