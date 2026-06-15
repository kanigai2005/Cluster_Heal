from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime

class PodInfo(BaseModel):
    name: str
    namespace: str
    node: str
    status: str
    restarts: int
    labels: Dict[str, str]

class MetricEvent(BaseModel):
    timestamp: float
    pod_key: str
    namespace: str
    pod_name: str
    cpu: float
    memory_mb: float
    io_kbps: float
    node: str

class AnomalyEvent(BaseModel):
    timestamp: float
    pod_key: str
    severity: str
    message: str
    current_cpu: float
    current_memory_mb: float

class RemediationCommand(BaseModel):
    command: str
    reason: str
    risk_level: str
