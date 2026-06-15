# anomaly_detection.py - ML anomaly detection
import numpy as np
from sklearn.ensemble import IsolationForest
from collections import deque
from typing import Dict, Tuple

class AnomalyDetector:
    def __init__(self, window_size=50):
        self.window_size = window_size
        self.history: Dict[str, deque] = {}
        self.models: Dict[str, IsolationForest] = {}
    
    def add_metric(self, pod_key: str, cpu: float, memory_mb: float):
        if pod_key not in self.history:
            self.history[pod_key] = deque(maxlen=self.window_size)
            self.models[pod_key] = IsolationForest(contamination=0.1, random_state=42)
            #Because anomalies are vastly different and rare, they require fewer splits to isolate (resulting in a short path length in the tree). Normal points cluster together and require more splits (longer path lengths). The algorithm calculates an anomaly score based on the average path length across all trees.

        self.history[pod_key].append([cpu, memory_mb])
        
        # Train when enough data
        if len(self.history[pod_key]) >= 30:
            X = np.array(self.history[pod_key])
            self.models[pod_key].fit(X)
    
    def detect(self, pod_key: str, cpu: float, memory_mb: float) -> Tuple[bool, float]:
        if pod_key not in self.models or len(self.history[pod_key]) < 30:
            return False, 0.0
        
        X = np.array([[cpu, memory_mb]])
        pred = self.models[pod_key].predict(X)[0]
        score = self.models[pod_key].score_samples(X)[0]
        
        return pred == -1, float(score)

anomaly_detector = AnomalyDetector()
