from pydantic import BaseModel, Field
from typing import List, Optional

class ProcessDetail(BaseModel):
    pid: int
    user: str
    cpu: float
    mem: float
    command: str

class PodTelemetry(BaseModel):
    name: str
    namespace: str
    deployment: str
    cpu_cores_used: float
    cpu_pct: float
    cpu_limit_cores: float
    memory_mb_used: float
    memory_limit_mb: float
    restarts: int
    status: str  # HEALTHY, WARNING, CRITICAL
    replicas: int
    is_anomaly: bool = False
    anomaly_score: float = 0.0
    creation_time_ms: int
    active_processes: List[ProcessDetail] = Field(default_factory=list)
    timestamp_ms: int

class AnomalyReport(BaseModel):
    pod_name: str
    deployment: str
    metric_type: str  # CPU, MEMORY, PROCESS
    anomaly_score: float
    timestamp_ms: int
    details: str

class RemediationAction(BaseModel):
    id: str
    timestamp: str
    pod_name: str
    namespace: str
    action: str  # scale_up, scale_down, restart, delete_pod, kill_process, do_nothing
    status: str  # SUCCESS, FAILED
    message: str
    triggered_by: str  # MANUAL, RL_AGENT
    reward: float
    notes: Optional[str] = None
