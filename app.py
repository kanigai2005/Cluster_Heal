import streamlit as st
import os
import time
import random
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import textwrap

def clean_html(html_str: str) -> str:
    """Removes all newlines and multiple leading/trailing spaces from HTML so Streamlit doesn't render it as a raw markdown code block."""
    lines = [line.strip() for line in html_str.splitlines()]
    return " ".join([line for line in lines if line])

# Import our custom modular files
import config
from anomaly_detection import SREAnomalyDetector
from dynamic_discovery import ClusterDiscoveryEngine
from k8s_client import KubernetesClientSimulator
from rl_optimizer import QLearningOptimizer, GroqSREAgent, ACTIONS
from kafka_producer import TelemetryProducerSimulator
from kafka_consumer import TelemetryConsumerSimulator

# ==========================================
# PAGE CONFIGURATION
# ==========================================
st.set_page_config(
    page_title="AI-Pod SRE Platform",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize modular helper classes in session state to persist state
if 'anomaly_detector' not in st.session_state:
    st.session_state.anomaly_detector = SREAnomalyDetector()
if 'discovery_engine' not in st.session_state:
    st.session_state.discovery_engine = ClusterDiscoveryEngine()
if 'k8s_client' not in st.session_state:
    st.session_state.k8s_client = KubernetesClientSimulator()
if 'rl_optimizer' not in st.session_state:
    st.session_state.rl_optimizer = QLearningOptimizer()
if 'groq_agent' not in st.session_state:
    st.session_state.groq_agent = GroqSREAgent()
if 'kafka_producer' not in st.session_state:
    st.session_state.kafka_producer = TelemetryProducerSimulator(config.KAFKA_BOOTSTRAP_SERVERS)
if 'kafka_consumer' not in st.session_state:
    st.session_state.kafka_consumer = TelemetryConsumerSimulator(config.KAFKA_BOOTSTRAP_SERVERS)

# Initialize standard state metrics & data structures
if 'pods' not in st.session_state:
    st.session_state.pods = []
    # Force initial discovery run
    st.session_state.pods = st.session_state.discovery_engine.discover_active_pods([])

# Pre-populate history if empty
if 'history_list' not in st.session_state:
    st.session_state.history_list = [
        {
            "id": "init-sre",
            "timestamp": time.strftime("%H:%M:%S", time.localtime(time.time() - 300)),
            "podName": st.session_state.pods[0]["name"] if st.session_state.pods else "esp-traffic-camera-abcde",
            "namespace": "town-traffic",
            "action": "restart",
            "status": "SUCCESS",
            "message": "Initialized autonomous platform. Baseline telemetry scrapes activated.",
            "triggeredBy": "SYSTEM",
            "reward": 5,
            "notes": "System checks: Isolation Forest calibrated, Prometheus scrapers online."
        }
    ]

# Other telemetry/AI state
if 'selected_pod_name' not in st.session_state and st.session_state.pods:
    st.session_state.selected_pod_name = st.session_state.pods[0]["name"]
if 'ai_recs_cache' not in st.session_state:
    st.session_state.ai_recs_cache = {}
if 'auto_remediation_enabled' not in st.session_state:
    st.session_state.auto_remediation_enabled = False
if 'epsilon' not in st.session_state:
    st.session_state.epsilon = config.RL_INITIAL_EPSILON
if 'total_steps' not in st.session_state:
    st.session_state.total_steps = 0
if 'ai_queries_count' not in st.session_state:
    st.session_state.ai_queries_count = 0
if 'last_update_time' not in st.session_state:
    st.session_state.last_update_time = time.time()
if 'kafka_msg_log' not in st.session_state:
    st.session_state.kafka_msg_log = []
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

# ==========================================
# CUSTOM THEMING & CSS STYLE INJECTIONS
# ==========================================
st.markdown("""
    <style>
    /* Dark Slate Slate Theme Styling with high readability */
    .stApp {
        background-color: #0d1117;
        color: #c9d1d9;
        font-family: 'Inter', sans-serif;
    }
    
    /* Increase basic stream lit font sizes globally */
    .stApp, p, span, li, label, .stText, .stMarkdown {
        font-size: 18px !important;
    }
    
    /* Make buttons larger and easier to click */
    .stButton > button {
        font-size: 18px !important;
        padding: 12px 24px !important;
        min-height: 48px !important;
    }
    
    /* Elegant Title Cards */
    .header-panel {
        background: linear-gradient(135deg, #161b22 0%, #21262d 100%);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 24px;
    }
    
    /* Neon Text Accents with larger font size */
    .neon-text-green {
        color: #39ff14;
        text-shadow: 0 0 10px rgba(57, 255, 20, 0.3);
        font-family: 'JetBrains Mono', monospace;
        font-size: 18px !important;
    }
    .neon-text-cyan {
        color: #00e5ff;
        text-shadow: 0 0 10px rgba(0, 229, 255, 0.3);
        font-family: 'JetBrains Mono', monospace;
        font-size: 18px !important;
    }
    .neon-text-red {
        color: #ff3b30;
        text-shadow: 0 0 10px rgba(255, 59, 48, 0.3);
        font-family: 'JetBrains Mono', monospace;
        font-size: 18px !important;
    }
    
    /* Containers with generous size and padding */
    .metric-box {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        transition: border-color 0.3s ease;
    }
    .metric-box:hover {
        border-color: #58a6ff;
    }
    
    /* Kubernetes Pod Boxes with comfortable padding and size */
    .pod-card {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 16px;
    }
    .pod-card-critical {
        background-color: #251216;
        border: 1px solid #f85149;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 16px;
        box-shadow: 0 0 12px rgba(248, 81, 73, 0.15);
    }
    .pod-card-warning {
        background-color: #221c10;
        border: 1px solid #d29922;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 16px;
    }
    
    /* Clean tables & monospace blocks with higher legibility */
    code, pre {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 13px !important;
    }
    h1 {
        font-size: 32px !important;
        font-family: 'JetBrains Mono', monospace !important;
        letter-spacing: -0.3px;
    }
    h2 {
        font-size: 26px !important;
        font-family: 'JetBrains Mono', monospace !important;
        letter-spacing: -0.3px;
    }
    h3 {
        font-size: 22px !important;
        font-family: 'JetBrains Mono', monospace !important;
        letter-spacing: -0.3px;
    }
    h4 {
        font-size: 18px !important;
        font-family: 'JetBrains Mono', monospace !important;
        letter-spacing: -0.3px;
    }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# SIMULATED ENVIRONMENT METRIC UPDATER
# ==========================================
def run_fluctuation_step():
    """
    Simulates real-time cAdvisor metric gathering step,
    applying chaotic stresses, scaling behaviors, memory leaks, and anomaly calculations.
    For real discovered Docker containers or host OS processes, it scrapes actual stats.
    """
    # Ensure active pods are discovered (real and/or simulated)
    st.session_state.pods = st.session_state.discovery_engine.discover_active_pods(st.session_state.pods)
    deleted_set = st.session_state.get('deleted_pods', set())
    if deleted_set:
        st.session_state.pods = [p for p in st.session_state.pods if p["name"] not in deleted_set]
    
    # Efficiently gather stats for real containers and processes in one go
    k8s_stats = st.session_state.discovery_engine.scrape_docker_stats() if st.session_state.discovery_engine.is_k8s_available else {}
    system_procs = {p["pid"]: p for p in st.session_state.discovery_engine.scrape_system_processes()} if not k8s_stats else {}
    
    for pod in st.session_state.pods:
        pod_name = pod["name"]
        
        # 1. Update from actual scraped live kubectl top metrics if available
        if pod_name in k8s_stats:
            stat = k8s_stats[pod_name]
            limit = max(0.1, pod.get("cpu_limit", 4.0))
            cpu_cores = stat.get("cpu_cores", (stat["cpu_pct"] / 100.0) * limit)
            cpu_pct = min(100.0, round((cpu_cores / limit) * 100.0, 1))
            
            pod["cpu_pct"] = cpu_pct
            pod["cpu"] = round(cpu_cores, 2)
            pod["memory_mb"] = stat["memory_mb"]
            pod["is_real"] = True
            
            if 'k8s_client' in st.session_state:
                st.session_state.k8s_client.update_pod_metrics(pod_name, cpu_pct=cpu_pct, memory_mb=stat["memory_mb"])
        elif pod.get("is_real", False) and pod.get("type") == "process":
            proc_info = system_procs.get(pod.get("pid"), None)
            if proc_info:
                pod["cpu_pct"] = min(100.0, proc_info["cpu"])
                pod["memory_mb"] = round((proc_info["mem"] / 100.0) * 4096.0, 1)
                pod["cpu"] = round((pod["cpu_pct"] / 100.0) * pod.get("cpu_limit", 4.0), 2)
        
        # Status check & anomaly score calculation
        mem_pct = (pod["memory_mb"] / max(1.0, pod["memory_limit"])) * 100.0
        if pod.get("status") in ["HEALTHY", "Running", "OK", "CRITICAL", "WARNING"]:
            if pod["cpu_pct"] >= config.CPU_CRITICAL_THRESHOLD_PCT or mem_pct >= config.MEM_CRITICAL_THRESHOLD_PCT or pod.get("is_stressed"):
                pod["status"] = "CRITICAL"
            elif pod["cpu_pct"] >= config.CPU_WARNING_THRESHOLD_PCT or mem_pct >= config.MEM_WARNING_THRESHOLD_PCT:
                pod["status"] = "WARNING"
            else:
                pod["status"] = "HEALTHY"
            
        # Isolation Forest Anomaly Analysis
        is_anomaly, score = st.session_state.anomaly_detector.analyze_metrics(pod["cpu_pct"], mem_pct)
        pod["isAnomaly"] = is_anomaly
        pod["anomalyScore"] = score
        
        # Time-series history
        if "history" not in pod:
            pod["history"] = []
        pod["history"].append({
            "cpu_pct": round(pod["cpu_pct"], 1),
            "memory_mb": round(pod["memory_mb"], 1),
            "timestamp": int(time.time() * 1000)
        })
        if len(pod["history"]) > 25:
            pod["history"].pop(0)

    # 1. Produce telemetry metrics to Kafka
    pushed_logs = st.session_state.kafka_producer.send_telemetry_batch(st.session_state.pods, config.KAFKA_TOPIC_TELEMETRY)
    st.session_state.kafka_msg_log.extend(pushed_logs)
    
    # PRINT sent messages to terminal as requested
    for log in pushed_logs:
        print(f"\033[36m[KAFKA-PRODUCER-STDOUT]\033[0m {log}", flush=True)

    # 2. Poll live telemetry from Kafka Consumer
    polled_msgs = st.session_state.kafka_consumer.poll_messages(config.KAFKA_TOPIC_TELEMETRY, limit=10)
    for msg in polled_msgs:
        t_str = msg.get("timestamp_str", time.strftime("%H:%M:%S"))
        src = msg.get("source", "SIMULATION")
        pod_name = msg.get("pod_name")
        
        import json
        print(f"\033[32m[KAFKA-CONSUMER-STDOUT]\033[0m RECV message on topic '{config.KAFKA_TOPIC_TELEMETRY}': {json.dumps(msg)}", flush=True)
        
        if msg.get("type") == "prometheus_adapter_metric":
            metric_name = msg.get("metric_name")
            metric_value = msg.get("metric_value")
            labels = msg.get("labels", {})
            log_str = f"[{t_str}] [{src}] RECV prometheus_metric='{metric_name}' value={metric_value} namespace='{msg.get('namespace')}'"
            
            # Map Prometheus metric value back to matching discovered pod if possible
            matched_pod = None
            if pod_name and pod_name not in ["unknown-pod", "generic-stream"]:
                matched_pod = next((p for p in st.session_state.pods if p["name"] == pod_name), None)
                
                dep_parts = pod_name.split("-")
                deployment_name = "-".join(dep_parts[:-2]) if (len(dep_parts) >= 3 and len(dep_parts[-1]) == 5 and dep_parts[-1].isalnum()) else pod_name
                
                if not matched_pod:
                    matched_pod = next((p for p in st.session_state.pods if p["deployment"] == deployment_name and not p.get("is_real", False)), None)
                    if matched_pod:
                        old_name = matched_pod["name"]
                        matched_pod["name"] = pod_name
                        matched_pod["is_real"] = True
                        matched_pod["is_kafka_updated"] = True
                        
                        if 'k8s_client' in st.session_state:
                            k8s_pod = next((p for p in st.session_state.k8s_client.pods if p["name"] == old_name), None)
                            if k8s_pod:
                                k8s_pod["name"] = pod_name
                                k8s_pod["is_real"] = True
                                k8s_pod["is_kafka_updated"] = True
                            if old_name in st.session_state.k8s_client.real_pod_metrics:
                                st.session_state.k8s_client.real_pod_metrics[pod_name] = st.session_state.k8s_client.real_pod_metrics.pop(old_name)
                
                if not matched_pod:
                    matched_pod = next((p for p in st.session_state.pods if pod_name.startswith(p["name"]) or p["name"].startswith(pod_name)), None)
                if not matched_pod:
                    matched_pod = next((p for p in st.session_state.pods if pod_name.startswith(p["deployment"]) or p["deployment"].startswith(pod_name)), None)
            
            # Compute parsed CPU / Memory value
            target_limit = matched_pod.get("cpu_limit", 4.0) if matched_pod else 4.0
            cpu_val = None
            mem_val = None
            if "cpu" in metric_name.lower():
                if metric_value > 100.0 and metric_value <= 10000.0:
                    cores = metric_value / 1000.0
                    cpu_val = min(100.0, round((cores / target_limit) * 100.0, 1))
                elif metric_value <= target_limit and metric_value <= 10.0:
                    cpu_val = min(100.0, round((metric_value / target_limit) * 100.0, 1))
                elif metric_value <= 1.0:
                    cpu_val = round(metric_value * 100.0, 1)
                else:
                    cpu_val = min(100.0, max(0.0, round(metric_value, 1)))
            elif "memory" in metric_name.lower() or "rss" in metric_name.lower():
                mem_val = round(metric_value / (1024.0 * 1024.0), 1) if metric_value > 1000000 else metric_value
            
            if 'k8s_client' in st.session_state:
                st.session_state.k8s_client.update_pod_metrics(pod_name, cpu_pct=cpu_val, memory_mb=mem_val)
                if matched_pod and matched_pod["name"] != pod_name:
                    st.session_state.k8s_client.update_pod_metrics(matched_pod["name"], cpu_pct=cpu_val, memory_mb=mem_val)

            if matched_pod:
                if cpu_val is not None:
                    matched_pod["cpu_pct"] = cpu_val
                    matched_pod["cpu"] = round((cpu_val / 100.0) * matched_pod.get("cpu_limit", 4.0), 2)
                if mem_val is not None:
                    matched_pod["memory_mb"] = mem_val
                matched_pod["is_kafka_updated"] = True
                
                mem_pct = (matched_pod["memory_mb"] / max(1.0, matched_pod["memory_limit"])) * 100.0
                if matched_pod["cpu_pct"] >= config.CPU_CRITICAL_THRESHOLD_PCT or mem_pct >= config.MEM_CRITICAL_THRESHOLD_PCT:
                    matched_pod["status"] = "CRITICAL"
                elif matched_pod["cpu_pct"] >= config.CPU_WARNING_THRESHOLD_PCT or mem_pct >= config.MEM_WARNING_THRESHOLD_PCT:
                    matched_pod["status"] = "WARNING"
                else:
                    matched_pod["status"] = "HEALTHY"
                is_anomaly, score = st.session_state.anomaly_detector.analyze_metrics(matched_pod["cpu_pct"], mem_pct)
                matched_pod["isAnomaly"] = is_anomaly
                matched_pod["anomalyScore"] = score
                    
        elif msg.get("type") == "sre_platform_telemetry":
            log_str = f"[{t_str}] [{src}] RECV key={pod_name} cpu={msg.get('cpu_pct')}% mem={msg.get('memory_mb')}MB status={msg.get('status')}"
            
            cpu_val = msg.get("cpu_pct")
            mem_val = msg.get("memory_mb")
            status_val = msg.get("status")
            
            if 'k8s_client' in st.session_state:
                st.session_state.k8s_client.update_pod_metrics(pod_name, cpu_pct=cpu_val, memory_mb=mem_val, status=status_val)
            
            matched_pod = next((p for p in st.session_state.pods if p["name"] == pod_name), None)

            if matched_pod:
                if cpu_val is not None:
                    matched_pod["cpu_pct"] = cpu_val
                if mem_val is not None:
                    matched_pod["memory_mb"] = mem_val
                if status_val is not None:
                    matched_pod["status"] = status_val
                matched_pod["is_kafka_updated"] = True
        else:
            log_str = f"[{t_str}] [{src}] RECV message on topic={msg.get('topic')} (Raw format)"
            
        st.session_state.kafka_msg_log.append(log_str)

    if len(st.session_state.kafka_msg_log) > 50:
        st.session_state.kafka_msg_log = st.session_state.kafka_msg_log[-50:]

    # AUTONOMOUS SRE HEALING ACTION TRIGGER
    if st.session_state.auto_remediation_enabled:
        # Find any CRITICAL pod
        crit_pods = [p for p in st.session_state.pods if p["status"] == "CRITICAL"]
        if crit_pods:
            target_pod = random.choice(crit_pods)
            state_key = st.session_state.rl_optimizer.get_state_key(target_pod)
            
            # Find valid actions
            valid_actions = []
            for act in ['force_delete', 'restart', 'scale_up', 'scale_down']:
                if act == 'scale_up' and target_pod.get("replicas", 1) >= 3:
                    continue
                if act == 'scale_down' and target_pod.get("replicas", 1) <= 1:
                    continue
                valid_actions.append(act)
                
            chosen_act = st.session_state.rl_optimizer.choose_action(state_key, st.session_state.epsilon, valid_actions)
            # Execute remediation
            execute_remediation(target_pod["name"], chosen_act, triggered_by="RL_AGENT")

# Sync periodic updates (runs fluctuation step every 3 seconds)
now = time.time()
elapsed = now - st.session_state.last_update_time
if elapsed >= config.SCRAPE_INTERVAL_SECONDS:
    run_fluctuation_step()
    st.session_state.last_update_time = now

# ==========================================
# EXECUTING REMEDIATION ACTION
# ==========================================
def execute_remediation(pod_name: str, action: str, triggered_by: str = "MANUAL"):
    # Normalize action name
    q_act = "force_delete" if action in ["force_delete", "delete_pod"] else action

    pod = next((p for p in st.session_state.pods if p["name"] == pod_name), None)
    if not pod:
        st.toast(f"Error: Target pod {pod_name} not found.", icon="⚠️")
        return
        
    prev_state_key = st.session_state.rl_optimizer.get_state_key(pod)
    message = ""
    success = True
    
    # 1. Print simulated Kubernetes API server log
    api_log = ""
    if q_act == "force_delete":
        old_name = pod["name"]
        api_log, _ = st.session_state.k8s_client.force_delete_pod(old_name, pod["namespace"])
        message = f"Force deleted pod {old_name}."
        if 'deleted_pods' not in st.session_state:
            st.session_state.deleted_pods = set()
        st.session_state.deleted_pods.add(old_name)
        st.session_state.pods = [p for p in st.session_state.pods if p["name"] != old_name]
        if 'k8s_client' in st.session_state:
            st.session_state.k8s_client.pods = [p for p in st.session_state.k8s_client.pods if p["name"] != old_name]
    elif q_act == "scale_up":
        api_log = st.session_state.k8s_client.scale_deployment(pod["deployment"], 3, pod["namespace"])
        message = f"Scaled deployment '{pod['deployment']}' horizontally to 3 replicas. Load balanced successfully."
        pod["is_stressed"] = False
        pod["status"] = "HEALTHY"
    elif q_act == "scale_down":
        api_log = st.session_state.k8s_client.scale_deployment(pod["deployment"], 1, pod["namespace"])
        message = f"Scaled deployment '{pod['deployment']}' down to 1 baseline replica."
    elif q_act == "restart":
        api_log = st.session_state.k8s_client.rollout_restart(pod["deployment"], pod["namespace"])
        message = f"rollout restart completed for {pod['deployment']}. Active thread buffers flushed."
        pod["is_stressed"] = False
        pod["cpu_pct"] = 12.0
        pod["memory_mb"] = 35.0
        pod["status"] = "HEALTHY"
        pod["activeProcesses"] = [{"pid": random.randint(100, 999), "user": "app", "cpu": 1.2, "mem": 2.5, "command": f"{pod['deployment']}-service"}]
    elif q_act == "do_nothing":
        api_log = f"No action taken for {pod['name']} as it is operating normally."
        message = "Pod is healthy. No remediation applied."

    # Clear discovery cache & immediately scrape fresh state
    st.session_state.discovery_engine.clear_cache()
    st.session_state.pods = st.session_state.discovery_engine.discover_active_pods(st.session_state.pods)

    # Sync selected_pod_name to an active pod if deleted/recreated
    active_names = [p["name"] for p in st.session_state.pods]
    if st.session_state.selected_pod_name not in active_names:
        same_dep_pod = next((p for p in st.session_state.pods if p["deployment"] == pod["deployment"]), None)
        if same_dep_pod:
            st.session_state.selected_pod_name = same_dep_pod["name"]
        elif active_names:
            st.session_state.selected_pod_name = active_names[0]

    # Append API log to Kafka events
    st.session_state.kafka_msg_log.append(f"[K8S-EVENT] {api_log}")
    
    # Calculate rewards for our policy table based on metric condition alignment
    reward = 0
    if success:
        cpu_val = pod.get("cpu_pct", 0.0)
        mem_val = (pod.get("memory_mb", 0.0) / max(1.0, pod.get("memory_limit", 512.0))) * 100.0
        
        if cpu_val > 70.0:
            if q_act == "scale_up":
                reward = 12
                message += " [SRE OPTIMIZED] Scaled out replicas to cushion load."
            elif q_act == "force_delete":
                reward = 8
                message += " [SRE REPLACED] Force deleted pod for clean replacement instance."
            elif q_act == "restart":
                reward = 6
                message += " [SRE RESTARTED] Rollout restarted deployment to clear load."
            else:
                reward = -5
        elif mem_val > 80.0:
            if q_act in ["restart", "force_delete"]:
                reward = 12
                message += " [SRE OPTIMIZED] Recycled pod memory footprint & cleared leak."
            elif q_act == "scale_up":
                reward = 4
                message += " [SRE BUFFER] Scale up added temporary memory overhead."
            else:
                reward = -5
        else:
            if q_act == "force_delete":
                reward = 10
            elif q_act == "restart":
                reward = 8
            else:
                reward = 2

    # Update Q-Learning weights table
    next_state_key = st.session_state.rl_optimizer.get_state_key(pod)
    new_q = st.session_state.rl_optimizer.update_q_value(prev_state_key, q_act, reward, next_state_key)
    
    # Epsilon decay
    if st.session_state.epsilon > 0.05:
        st.session_state.epsilon = round(st.session_state.epsilon - config.RL_DECAY_RATE, 3)
        
    st.session_state.total_steps += 1
    
    # Record history logs
    action_log = {
        "id": ''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(7)),
        "timestamp": time.strftime("%H:%M:%S"),
        "podName": pod["name"],
        "namespace": pod["namespace"],
        "action": action,
        "status": "SUCCESS" if success else "FAILED",
        "message": message,
        "triggeredBy": triggered_by,
        "reward": reward,
        "notes": f"Q-value ({prev_state_key} | {action}) updated to {new_q}."
    }
    
    st.session_state.history_list.insert(0, action_log)
    st.session_state.discovery_engine.clear_cache()
    st.toast(f"Executed action: {action.upper()} ({triggered_by})", icon="⚡")

# ==========================================
# INJECTING CHAOS LOAD TO TRIGGER ALERTS
# ==========================================
def inject_chaos_load(pod_name: str):
    pod = next((p for p in st.session_state.pods if p["name"] == pod_name), None)
    if not pod:
        return
        
    k8s_pod = next((p for p in st.session_state.k8s_client.pods if p["name"] == pod_name), None)
    
    # Randomly select a stress type: CPU stress or Memory leak
    stress_type = random.choice(["cpu", "memory"])
    
    # Mutate both references so they stay perfectly in sync
    for p_ref in [pod, k8s_pod]:
        if not p_ref:
            continue
        p_ref["is_stressed"] = True
        
        if stress_type == "cpu":
            p_ref["cpu_pct"] = 96.4
            p_ref["status"] = "CRITICAL"
            p_ref["isAnomaly"] = True
            p_ref["anomalyScore"] = -0.78
            # Add stress process
            if not any("stress" in p["command"] for p in p_ref.get("activeProcesses", [])):
                p_ref["activeProcesses"].append({
                    "pid": random.randint(100, 999), "user": "root", "cpu": 94.2, "mem": 1.2, "command": "stress --cpu 4 --timeout 1800"
                })
        else:
            p_ref["cpu_pct"] = 42.0
            p_ref["memory_mb"] = round(p_ref["memory_limit"] * 0.91, 1)
            p_ref["status"] = "CRITICAL"
            p_ref["isAnomaly"] = True
            p_ref["anomalyScore"] = -0.82
            # Add memory leak java process
            if not any("leak" in p["command"] or "java" in p["command"] for p in p_ref.get("activeProcesses", [])):
                p_ref["activeProcesses"].append({
                    "pid": random.randint(100, 999), "user": "root", "cpu": 12.0, "mem": 88.0, "command": "java -Xmx384m -jar memory-leak.jar"
                })
            
    st.session_state.kafka_msg_log.append(f"[CHAOS-AGENT] Injected stress profiles into deployment {pod['deployment']}")
    st.session_state.discovery_engine.clear_cache()
    st.toast(f"Injected Chaos Stress into {pod['deployment']}!", icon="🔥")

# ==========================================
# RESET METHOD
# ==========================================
def purge_simulator_state():
    st.session_state.epsilon = config.RL_INITIAL_EPSILON
    st.session_state.total_steps = 0
    st.session_state.ai_queries_count = 0
    st.session_state.auto_remediation_enabled = False
    st.session_state.rl_optimizer.reset_q_table()
    st.session_state.ai_recs_cache = {}
    st.session_state.kafka_msg_log = []
    st.session_state.deleted_pods = set()
    
    st.session_state.k8s_client = KubernetesClientSimulator()
    st.session_state.pods = st.session_state.discovery_engine.discover_active_pods([])
    st.session_state.selected_pod_name = st.session_state.pods[0]["name"]
    st.session_state.history_list = [
        {
            "id": "reset-event",
            "timestamp": time.strftime("%H:%M:%S"),
            "podName": st.session_state.pods[0]["name"],
            "namespace": "town-traffic",
            "action": "restart",
            "status": "SUCCESS",
            "message": "Policy purge successfully completed. SRE model parameters reset.",
            "triggeredBy": "SYSTEM",
            "reward": 0,
            "notes": "Q-Learning weights and telemetry histories deleted."
        }
    ]
    st.toast("Simulator database reset successfully!", icon="🔄")

# ==========================================
# AUTHENTICATION ACCESS CONTROL (LOGIN PAGE)
# ==========================================
if not st.session_state.get('logged_in', False):
    st.markdown("""
        <div style="max-width: 600px; margin: 40px auto; padding: 30px; background-color: #161b22; border: 1px solid #30363d; border-radius: 12px; box-shadow: 0 8px 32px rgba(0,0,0,0.5); text-align: center;">
            <span style="font-size: 60px; filter: drop-shadow(0 0 15px rgba(88,166,255,0.4));">🤖</span>
            <h1 style="color: #ffffff; font-size: 28px; margin-top: 15px; margin-bottom: 5px; font-family: 'Inter', sans-serif;">SRE KUBERNETES PORTAL</h1>
            <p style="color: #8b949e; font-size: 14px; font-family: 'JetBrains Mono', monospace; letter-spacing: 0.5px;">AUTONOMOUS AGENT REINFORCEMENT LEARNING DECK</p>
            <div style="height: 1px; background-color: #30363d; margin: 25px 0;"></div>
            <p style="color: #c9d1d9; font-size: 15px; margin-bottom: 25px; line-height: 1.5; font-family: 'Inter', sans-serif;">
                Access is restricted to authorized Site Reliability Engineers. Please authenticate using your secure access keys to decrypt telemetry pipelines and activate the Q-learning policy tables.
            </p>
        </div>
    """, unsafe_allow_html=True)
    
    col_l, col_c, col_r = st.columns([1.1, 2.0, 1.1])
    with col_c:
        with st.form("login_form"):
            st.markdown("<h4 style='text-align: center; color: #58a6ff; font-family: monospace; margin-bottom: 15px;'>🛡️ CREDENTIALS CHECKPOINT</h4>", unsafe_allow_html=True)
            username = st.text_input("Username / SRE Operator ID", value="admin", help="Default credentials: admin")
            password = st.text_input("Access Control Password", type="password", value="sre-password", help="Default credentials: sre-password")
            submitted = st.form_submit_button("🔑 DECRYPT & INITIALIZE CONSOLE", width='stretch')
            
            if submitted:
                if username == "admin" and password == "sre-password":
                    st.session_state.logged_in = True
                    st.success("Authorization successful! Fetching real-time container metrics...")
                    st.toast("Welcome back, SRE Engineer!", icon="🔑")
                    time.sleep(1.0)
                    st.rerun()
                else:
                    st.error("Access Denied: Invalid operator identity or security passkey.")
                    
    # Prevent rendering the main portal under unauthorized sessions
    st.stop()

# ==========================================
# SIDEBAR / CONFIGURATION WINDOW
# ==========================================
with st.sidebar:
    st.markdown("""
        <div style="text-align: center; margin-bottom: 20px;">
            <span style="font-size: 24px;">🤖</span>
            <h2 style="margin: 10px 0 0 0; font-size: 16px; color: #fff;">SRE PLATFORM</h2>
            <span style="font-size: 10px; color: #8b949e; font-family: monospace;">K8s Agent Control v1.3</span>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("### ⚙️ Live Automation Settings")
    
    # Toggle Auto Healing
    auto_enabled = st.toggle(
        "Auto-Heal Agent", 
        value=st.session_state.auto_remediation_enabled,
        help="Permit Q-Learning Agent to apply remedies autonomously on alert triggers."
    )
    if auto_enabled != st.session_state.auto_remediation_enabled:
        st.session_state.auto_remediation_enabled = auto_enabled
        st.rerun()
        
    loop_enabled = st.checkbox("Scrape Loop (3s)", value=True, help="Simulate real-time prometheus queries")
    

    
    st.markdown("---")
    st.markdown("### 🛠️ Danger Zone")
    if st.button("🔄 Purge Policy & State", help="Purge Q-Table, Alerts, and Pod Telemetries"):
        purge_simulator_state()
        st.rerun()

# ==========================================
# TITLE BLOCK
# ==========================================
st.markdown(clean_html(f"""
    <div class="header-panel">
        <div style="display: flex; align-items: center; gap: 16px;">
            <div style="background: linear-gradient(135deg, #58a6ff 0%, #00e5ff 100%); padding: 10px; border-radius: 8px;">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#0d1117" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
                    <rect x="2" y="2" width="20" height="8" rx="2" ry="2"></rect>
                    <rect x="2" y="14" width="20" height="8" rx="2" ry="2"></rect>
                    <line x1="6" y1="6" x2="6.01" y2="6"></line>
                    <line x1="6" y1="18" x2="6.01" y2="18"></line>
                </svg>
            </div>
            <div>
                <div style="display: flex; align-items: center; gap: 8px;">
                    <h1 style="margin: 0; font-size: 20px; color: #ffffff;">AUTONOMOUS AI-POD SRE PORTAL</h1>
                    <span style="font-size: 10px; background-color: #21262d; color: #58a6ff; border: 1px solid #30363d; padding: 2px 6px; border-radius: 4px; font-family: monospace;">PY-STRE-1.3</span>
                </div>
                <p style="margin: 4px 0 0 0; font-size: 12px; color: #8b949e;">Kubernetes Namespace Control Deck • Reinforcement Learning Optimization Engine • Groq Llama-3.3 Diagnostic Copilot</p>
            </div>
        </div>
    </div>
"""), unsafe_allow_html=True)

# Quick stats summary row
total_p = len(st.session_state.pods)
crit_p = len([p for p in st.session_state.pods if p["status"] == "CRITICAL"])
warn_p = len([p for p in st.session_state.pods if p["status"] == "WARNING"])
ok_p = total_p - crit_p - warn_p

total_rewardable = len([h for h in st.session_state.history_list if h.get("reward", 0) != 0])
positive_rewards = len([h for h in st.session_state.history_list if h.get("reward", 0) > 0])
sla_pct = round((positive_rewards / total_rewardable) * 100) if total_rewardable > 0 else 100

st_col1, st_col2, st_col3, st_col4, st_col5 = st.columns([1.2, 1, 1.2, 1, 0.4])

with st_col1:
    st.markdown(f"""
        <div class="metric-box">
            <span style="font-size: 13px; color: #8b949e; font-family: monospace; font-weight: bold;">CLUSTER DISCOVERY</span>
            <div style="font-size: 22px; font-weight: bold; margin-top: 6px; font-family: monospace; color: #ffffff;">{ok_p} / {total_p} ONLINE</div>
        </div>
    """, unsafe_allow_html=True)

with st_col2:
    status_indicator = '<span class="neon-text-green">NOMINAL</span>' if crit_p == 0 else f'<span class="neon-text-red">{crit_p} UNHEALTHY</span>'
    st.markdown(f"""
        <div class="metric-box">
            <span style="font-size: 13px; color: #8b949e; font-family: monospace; font-weight: bold;">ACTIVE INCIDENTS</span>
            <div style="font-size: 22px; margin-top: 6px; font-family: monospace;">{status_indicator}</div>
        </div>
    """, unsafe_allow_html=True)

with st_col3:
    color = "#39ff14" if sla_pct >= 85 else "#ff3b30"
    st.markdown(f"""
        <div class="metric-box">
            <span style="font-size: 13px; color: #8b949e; font-family: monospace; font-weight: bold;">SRE REWARD COMPLIANCE</span>
            <div style="font-size: 22px; font-weight: bold; margin-top: 6px; font-family: monospace; color: {color};">{sla_pct}%</div>
        </div>
    """, unsafe_allow_html=True)

with st_col4:
    ai_status = '<span class="neon-text-cyan">ACTIVE</span>' if st.session_state.auto_remediation_enabled else '<span style="color:#8b949e;">STANDBY</span>'
    st.markdown(f"""
        <div class="metric-box">
            <span style="font-size: 13px; color: #8b949e; font-family: monospace; font-weight: bold;">AUTO-PILOT AGENT</span>
            <div style="font-size: 22px; font-weight: bold; margin-top: 6px; font-family: monospace;">{ai_status}</div>
        </div>
    """, unsafe_allow_html=True)

with st_col5:
    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    if st.button("🔄 Scrape", help="Perform immediate Prometheus poll & cluster auto-discovery"):
        st.session_state.discovery_engine.clear_cache()
        run_fluctuation_step()
        st.rerun()

st.markdown("<div style='height: 15px;'></div>", unsafe_allow_html=True)

# Define 3 windows using Tabs layout as requested
tab_telemetry, tab_remediation, tab_rl = st.tabs([
    "📊 Window 1: Telemetry & Stream Dashboard",
    "⚡ Window 2: Remediation & AI Recommendations",
    "🧠 Window 3: RL Policy Insights & Configuration"
])

# ==========================================
# WINDOW 1: TELEMETRY & KAFKA STREAM DASHBOARD
# ==========================================
with tab_telemetry:
    st.markdown("### Kubernetes Discovered Pods Node Grid")
    
    # 2x2 grid for discovered pods
    p_cols = st.columns(4)
    for idx, pod in enumerate(st.session_state.pods):
        with p_cols[idx % 4]:
            card_style = "pod-card"
            if pod["status"] in ["CRITICAL", "Error in image pulling", "ErrImagePull", "ImagePullBackOff"]:
                card_style = "pod-card-critical"
            elif pod["status"] in ["WARNING", "STOPPED"]:
                card_style = "pod-card-warning"
                
            status_color = "#39ff14" if pod["status"] == "HEALTHY" else "#d29922" if pod["status"] in ["WARNING", "STOPPED"] else "#f85149"
            anomaly_lbl = f"<span style='color: #f85149; font-weight:bold;'>Anomaly ({pod['anomalyScore']})</span>" if pod["isAnomaly"] else f"<span style='color: #39ff14;'>Nominal ({pod['anomalyScore']})</span>"
            
            # Format creation age
            created_ts = pod.get("creationTime", int(time.time() * 1000))
            age_sec = max(0, int(time.time() - (created_ts / 1000.0)))
            if age_sec < 60:
                age_str = f"{age_sec}s"
            elif age_sec < 3600:
                age_str = f"{age_sec // 60}m"
            else:
                age_str = f"{age_sec // 3600}h"

            full_status = pod["status"]

            st.markdown(clean_html(f"""
                <div class="{card_style}">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                        <span style="font-size: 12px; background-color: #21262d; border: 1px solid #30363d; padding: 4px 10px; border-radius: 6px; font-family: monospace; color: #8b949e;">{pod['namespace']}</span>
                        <strong style="color: {status_color}; font-size: 12px; text-transform: uppercase; border: 1px solid {status_color}50; padding: 3px 8px; border-radius: 12px; background-color: {status_color}10; white-space: nowrap; max-width: 150px; overflow: hidden; text-overflow: ellipsis;" title="{full_status}">{full_status}</strong>
                    </div>
                    <div style="margin-bottom: 14px;">
                        <h4 style="margin: 0 0 4px 0; font-size: 15px; color: #f0f6fc; text-overflow: ellipsis; white-space: nowrap; overflow: hidden;" title="{pod['name']}">{pod['name']}</h4>
                        <div style="font-size: 12px; color: #8b949e; margin-bottom: 2px;">Deployment: <strong style="color: #c9d1d9;">{pod['deployment']}</strong></div>
                        <div style="font-size: 12px; color: #8b949e;">Pod Status: <strong style="color: {status_color}; font-weight: bold; background-color: {status_color}15; padding: 2px 6px; border-radius: 4px; border: 1px solid {status_color}40;">{full_status}</strong></div>
                    </div>
                    
                    <!-- CPU Metric Gauge -->
                    <div style="margin-bottom: 14px;">
                        <div style="display: flex; justify-content: space-between; font-size: 12px; font-family: monospace; margin-bottom: 4px;">
                            <span style="color: #8b949e;">CPU utilization:</span>
                            <strong style="color: {status_color};">{pod['cpu_pct']:.1f}%</strong>
                        </div>
                        <div style="background-color: #30363d; height: 8px; border-radius: 4px; overflow: hidden;">
                            <div style="background-color: {status_color}; width: {min(100.0, pod['cpu_pct'])}%; height: 100%;"></div>
                        </div>
                        <div style="display: flex; justify-content: space-between; font-size: 11px; color: #8b949e; font-family: monospace; margin-top: 3px;">
                            <span>{pod['cpu']:.2f} Cores</span>
                            <span>Limit: {pod['cpu_limit']} Cores</span>
                        </div>
                    </div>

                    <!-- Memory Metric Gauge -->
                    <div style="margin-bottom: 16px;">
                        <div style="display: flex; justify-content: space-between; font-size: 12px; font-family: monospace; margin-bottom: 4px;">
                            <span style="color: #8b949e;">Memory allocation:</span>
                            <strong style="color: #58a6ff;">{int((pod['memory_mb'] / max(1.0, pod['memory_limit'])) * 100.0)}%</strong>
                        </div>
                        <div style="background-color: #30363d; height: 8px; border-radius: 4px; overflow: hidden;">
                            <div style="background-color: #58a6ff; width: {min(100.0, (pod['memory_mb'] / max(1.0, pod['memory_limit'])) * 100.0)}%; height: 100%;"></div>
                        </div>
                        <div style="display: flex; justify-content: space-between; font-size: 11px; color: #8b949e; font-family: monospace; margin-top: 3px;">
                            <span>{pod['memory_mb']:.1f} MB</span>
                            <span>Limit: {pod['memory_limit']} MB</span>
                        </div>
                    </div>

                    <!-- Extra Stats -->
                    <div style="display: flex; gap: 6px; margin-bottom: 14px; background-color: #0d1117; padding: 8px; border-radius: 8px; border: 1px solid #21262d; justify-content: space-between; text-align: center;">
                        <div style="flex:1;">
                            <span style="font-size: 10px; color: #8b949e; display:block; font-family: monospace; font-weight: bold;">REPLICAS</span>
                            <strong style="font-size: 13px; color: #c9d1d9; font-family: monospace;">{pod['replicas']}/3</strong>
                        </div>
                        <div style="flex:1; border-left: 1px solid #21262d; border-right: 1px solid #21262d;">
                            <span style="font-size: 10px; color: #8b949e; display:block; font-family: monospace; font-weight: bold;">RESTARTS</span>
                            <strong style="font-size: 13px; color: #c9d1d9; font-family: monospace;">{pod['restarts']}</strong>
                        </div>
                        <div style="flex:1;">
                            <span style="font-size: 10px; color: #8b949e; display:block; font-family: monospace; font-weight: bold;">STATE</span>
                            <strong style="font-size: 12px; color: {status_color}; font-family: monospace; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; display: block;" title="{full_status}">{full_status}</strong>
                        </div>
                    </div>

                    <!-- Isolation score & Age -->
                    <div style="display: flex; justify-content: space-between; align-items: center; font-size: 11px; font-family: monospace; border-top: 1px solid #21262d; padding-top: 8px;">
                        <span style="color: #8b949e;">Isolation: {anomaly_lbl}</span>
                        <span style="color: #8b949e;">Age: {age_str}</span>
                    </div>
                </div>
            """), unsafe_allow_html=True)
            
            # Details Expander right on card
            with st.expander(f"📋 Pod Specs & Processes ({pod['name'][:18]}...)", expanded=False):
                st.markdown(f"""
                - **Pod Name:** `{pod['name']}`
                - **Namespace:** `{pod['namespace']}`
                - **Deployment:** `{pod['deployment']}`
                - **Status:** `{full_status}`
                - **CPU Limit:** `{pod['cpu_limit']} Cores` | **Mem Limit:** `{pod['memory_limit']} MB`
                - **Type:** `{pod.get('type', 'k8s_pod')}`
                """)
                procs = pod.get("activeProcesses", [])
                if procs:
                    st.markdown("**Active Container Processes (ps aux):**")
                    for pr in procs:
                        st.markdown(f"- PID `{pr.get('pid')}` (`{pr.get('user')}`): CPU `{pr.get('cpu')}%` | Mem `{pr.get('mem')}%` → `{pr.get('command')}`")
                else:
                    st.caption("No active user processes inside container.")
            
            # Button layouts
            b_col1, b_col2 = st.columns(2)
            with b_col1:
                if st.button("🔄 Restart Pod", key=f"t-restart-{pod['name']}", help="Trigger container restart"):
                    execute_remediation(pod["name"], "restart")
                    st.rerun()
            with b_col2:
                if st.button("🔍 Diagnose", key=f"t-diag-{pod['name']}", help="Analyze processes & logs"):
                    st.session_state.selected_pod_name = pod["name"]
                    st.toast(f"Diagnosing {pod['name']}", icon="🔍")
                    st.rerun()

    st.markdown("---")
    
    # Graphs and Active Processes layout row
    vis_col1, vis_col2 = st.columns([1.5, 1])
    
    with vis_col1:
        st.markdown("### 📈 Live Prometheus Telemetry Scraping (Time-Series)")
        chosen_graph_pod_name = st.selectbox(
            "Select K8s pod targeted for telemetry graph:",
            options=[p["name"] for p in st.session_state.pods],
            key="graph_selector"
        )
        graph_pod = next((p for p in st.session_state.pods if p["name"] == chosen_graph_pod_name), None)
        if graph_pod and "history" in graph_pod and len(graph_pod["history"]) > 0:
            df = pd.DataFrame(graph_pod["history"])
            df["Time"] = df["timestamp"].apply(lambda ts: time.strftime("%H:%M:%S", time.localtime(ts / 1000.0)))
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df["Time"], y=df["cpu_pct"],
                mode='lines+markers', name='CPU Core Load %',
                line=dict(color='#39ff14', width=2),
                marker=dict(color='#39ff14', size=4)
            ))
            fig.add_trace(go.Scatter(
                x=df["Time"], y=df["memory_mb"],
                mode='lines+markers', name='Memory Used MB',
                line=dict(color='#58a6ff', width=2),
                marker=dict(color='#58a6ff', size=4),
                yaxis='y2'
            ))
            
            fig.update_layout(
                paper_bgcolor='#161b22',
                plot_bgcolor='#161b22',
                margin=dict(l=15, r=15, t=10, b=10),
                height=230,
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color="#8b949e", size=10)),
                font=dict(color='#8b949e', family='JetBrains Mono'),
                xaxis=dict(gridcolor='#21262d', zerolinecolor='#21262d', showticklabels=True),
                yaxis=dict(title=dict(text='CPU %', font=dict(color='#39ff14')), gridcolor='#21262d', zerolinecolor='#21262d'),
                yaxis2=dict(title=dict(text='Memory MB', font=dict(color='#58a6ff')), overlaying='y', side='right', gridcolor='#21262d', showgrid=False)
            )
            st.plotly_chart(fig, width='stretch')

    with vis_col2:
        st.markdown("### 🧬 active Container Process Monitor (ps aux)")
        diag_pod = next((p for p in st.session_state.pods if p["name"] == chosen_graph_pod_name), None)
        if diag_pod:
            processes = diag_pod.get("activeProcesses", [])
            p_df = pd.DataFrame(processes)
            if not p_df.empty:
                p_df.columns = ["PID", "User", "CPU %", "Mem %", "Command"]
                st.dataframe(p_df, hide_index=True, width='stretch', height=220)
            else:
                st.info("No active processes detected in this container core.")
                
    st.markdown("---")
    st.markdown("### 📻 Real-time Telemetry Event Log (Kafka Message Stream)")
    
    # Visual Kafka Health Monitor Checkpoint
    kafka_connected = st.session_state.kafka_producer.connected and st.session_state.kafka_consumer.connected
    broker_address = config.KAFKA_BOOTSTRAP_SERVERS
    
    status_text = "💚 KAFKA BROKER ONLINE & ACTIVE" if kafka_connected else "💻 EMBEDDED TELEMETRY PIPELINE NOMINAL"
    status_color = "#39ff14" if kafka_connected else "#00e5ff"
    
    st.markdown(f"""
        <div style="background-color: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; margin-bottom: 16px;">
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
                <div>
                    <span style="font-size: 13px; color: #8b949e; font-family: monospace; font-weight: bold;">KAFKA CLUSTER STATUS</span>
                    <div style="font-size: 18px; font-weight: bold; font-family: monospace; color: {status_color};">{status_text}</div>
                </div>
                <div style="text-align: right; min-width: 150px;">
                    <span style="font-size: 13px; color: #8b949e; font-family: monospace; font-weight: bold;">BOOTSTRAP SERVER</span>
                    <div style="font-size: 14px; font-family: monospace; color: #c9d1d9;">{broker_address}</div>
                </div>
            </div>
            <div style="margin-top: 10px; font-size: 14px; color: #8b949e; font-family: 'Inter', sans-serif; line-height: 1.4;">
                {"Real-time confluent cp-kafka driver is fully initialized and streaming live container metrics to the SRE console." if kafka_connected else f"The applet is executing the robust telemetry pipeline. Since a raw Kafka container is not running on this sandbox's host (Docker service not available), the pipeline automatically initialized the built-in high-fidelity Telemetry Bus to stream metrics safely without crashing."}
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    # Display recent logs in terminal block
    if st.session_state.kafka_msg_log:
        recent_kafka_logs = "\n".join(st.session_state.kafka_msg_log[-12:])
        st.code(recent_kafka_logs, language="bash")
    else:
        st.info("Kafka queue is currently empty. Awaiting telemetry producer scrape inputs...")

# ==========================================
# WINDOW 2: REMEDIATION & AI RECOMMENDATIONS
# ==========================================
with tab_remediation:
    rem_col1, rem_col2 = st.columns([1.2, 1])
    
    with rem_col1:
        st.markdown("### 🛠️ Interactive Remediation Execution Matrix")
        
        target_remedy_pod_name = st.selectbox(
            "Select K8s Pod to Target for Policy Action:",
            options=[p["name"] for p in st.session_state.pods],
            key="remedy_target_selector"
        )
        
        target_remedy_pod = next((p for p in st.session_state.pods if p["name"] == target_remedy_pod_name), None)
        if target_remedy_pod:
            st.markdown(f"**Target Pod Specs:** `{target_remedy_pod['name']}` • Namespace: `{target_remedy_pod['namespace']}` • Deployment: `{target_remedy_pod['deployment']}`")
            
            # Action button matrix grid - 4 core K8s SRE actions
            act_cols = st.columns(2)
            with act_cols[0]:
                if st.button("🗑️ Force Delete Pod", width='stretch', help="Immediately delete pod instance and trigger replica replacement"):
                    execute_remediation(target_remedy_pod_name, "force_delete")
                    st.rerun()
                if st.button("⚡ Scale Up (3x)", width='stretch', help="Scale deployment up to 3 replicas"):
                    execute_remediation(target_remedy_pod_name, "scale_up")
                    st.rerun()
            with act_cols[1]:
                if st.button("🔌 Rollout Restart", width='stretch', help="Rollout restart deployment instances"):
                    execute_remediation(target_remedy_pod_name, "restart")
                    st.rerun()
                if st.button("📉 Scale Down (1x)", width='stretch', help="Scale deployment down to 1 baseline replica"):
                    execute_remediation(target_remedy_pod_name, "scale_down")
                    st.rerun()

            st.markdown("<div style='height: 15px;'></div>", unsafe_allow_html=True)
            st.markdown("### 📊 Active Q-Learning Policy Table (Q-Table)")
            st.markdown("Q-Table policy rewards and action weights for states: `force_delete`, `restart`, `scale_up`, `scale_down`.")
            
            # Draw Q-Table directly in Remediation Tab
            q_table_data = []
            for state_key, actions_dict in st.session_state.rl_optimizer.q_table.items():
                row_dict = {"SRE Telemetry State": state_key}
                for act in ['force_delete', 'restart', 'scale_up', 'scale_down', 'do_nothing']:
                    row_dict[act] = actions_dict.get(act, 0.0)
                q_table_data.append(row_dict)
                
            if q_table_data:
                q_df = pd.DataFrame(q_table_data)
                st.dataframe(q_df, hide_index=True, width='stretch', height=220)
            else:
                sample_states = [
                    {"SRE Telemetry State": "water_high_cpu_ok_mem", "force_delete": 8.0, "restart": 6.0, "scale_up": 12.0, "scale_down": -5.0, "do_nothing": 0.0},
                    {"SRE Telemetry State": "kitchen_med_cpu_high_mem", "force_delete": 12.0, "restart": 12.0, "scale_up": 4.0, "scale_down": -5.0, "do_nothing": 0.0},
                    {"SRE Telemetry State": "traffic_ok_cpu_ok_mem", "force_delete": 10.0, "restart": 8.0, "scale_up": 2.0, "scale_down": 2.0, "do_nothing": 10.0}
                ]
                q_df = pd.DataFrame(sample_states)
                st.dataframe(q_df, hide_index=True, width='stretch', height=180)
                st.info("Q-Table initialized with baseline heuristics. Live remediation actions will update Q-values above in real time.")

    with rem_col2:
        st.markdown("### 🧠 SRE Diagnostic Advisor & Q-Table Ranked Recommendations")
        st.markdown("AI Recommendations are dynamically ranked based on the selected pod's real-time telemetry state and Q-Learning policy table.")
        
        ai_target_pod_name = st.selectbox(
            "Select targeted container pod to retrieve Q-table ranked recipes:",
            options=[p["name"] for p in st.session_state.pods],
            key="ai_target_selector"
        )
        ai_target_pod = next((p for p in st.session_state.pods if p["name"] == ai_target_pod_name), None)
        
        if ai_target_pod:
            if st.button("🔮 Ask Groq Copilot & Refresh Q-Rankings", help="Query Groq SRE diagnostic API and map to Q-Table"):
                with st.spinner("Analyzing container telemetry logs, CPU metrics, and Q-Table policy weights..."):
                    recs = st.session_state.groq_agent.get_recommendations(ai_target_pod)
                    st.session_state.ai_recs_cache[ai_target_pod["name"]] = recs
                    st.session_state.ai_queries_count += 1
            
            # Retrieve or generate base recommendations
            base_recs = st.session_state.ai_recs_cache.get(ai_target_pod["name"], None)
            if not base_recs:
                base_recs = st.session_state.groq_agent.get_recommendations(ai_target_pod)
                st.session_state.ai_recs_cache[ai_target_pod["name"]] = base_recs
                
            # Rank recommendations using Q-Table for individual target pod
            ranked_recs = st.session_state.groq_agent.rank_recommendations(
                ai_target_pod, base_recs, st.session_state.rl_optimizer
            )
            
            target_state_key = st.session_state.rl_optimizer.get_state_key(ai_target_pod)
            st.markdown(f"#### 🏆 Ranked Remediation Prescriptions for `{ai_target_pod['name']}` (State: `{target_state_key}`):")
            
            for rec in ranked_recs:
                rank = rec.get("rank", 1)
                action = rec.get("action", "restart").replace("_", " ").upper()
                impact = rec.get("impact", "medium").upper()
                reason = rec.get("reason", "")
                cmd = rec.get("kubectl_command", "")
                q_score = rec.get("q_score", 0.0)
                
                impact_color = "#f85149" if impact == "HIGH" else "#d29922" if impact == "MEDIUM" else "#58a6ff"
                rank_lbl = "🏆 RANK #1 (BEST REMEDY)" if rank == 1 else f"RANK #{rank}"
                rank_color = "#39ff14" if rank == 1 else "#00e5ff"
                
                st.markdown(clean_html(f"""
                    <div style="background-color: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 18px; margin-bottom: 14px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                            <div>
                                <strong style="color: {rank_color}; font-family: monospace; font-size: 15px;">{rank_lbl}: {action}</strong>
                                <span style="font-size: 12px; color: #39ff14; font-family: monospace; margin-left: 10px; font-weight: bold;">[Q-Score: {q_score:+.1f}]</span>
                            </div>
                            <span style="font-size: 11px; font-weight: bold; font-family: monospace; border: 1px solid {impact_color}; color: {impact_color}; padding: 3px 8px; border-radius: 6px; background-color: rgba(0,0,0,0.15);">
                                {impact} IMPACT
                            </span>
                        </div>
                        <p style="margin: 0; font-size: 14px; color: #c9d1d9;">{reason}</p>
                        <div style="margin-top: 10px; font-family: monospace; font-size: 13px; background-color: #0d1117; padding: 10px; border-radius: 6px; border: 1px solid #21262d; color: #58a6ff;">
                            {cmd}
                        </div>
                    </div>
                """), unsafe_allow_html=True)
                
                # Direct click healing button
                raw_act = rec.get("action", "restart")
                if st.button(f"Apply Rank #{rank}: {raw_act.replace('_', ' ').title()} (Q: {q_score:+.1f})", key=f"apply-{rank}-{ai_target_pod['name']}"):
                    execute_remediation(ai_target_pod["name"], raw_act, "AI_COPILOT")
                    st.rerun()

# ==========================================
# WINDOW 3: RL POLICY INSIGHTS & CONFIGURATION
# ==========================================
with tab_rl:
    st.markdown("### Autonomous Site-Reliability Reinforcement Learning Policy Matrix")
    
    rl_col1, rl_col2 = st.columns([1.3, 1])
    
    with rl_col1:
        st.markdown("#### Policy Q-Table Visual Grid")
        st.markdown("States represent cluster telemetry levels (Deployment_CPU_Memory). Actions modify pod topologies. Higher Q-Values highlight SRE choices that minimize downtime and maximize stability.")
        
        # Draw tabular grid representation of current policy
        q_table_data = []
        for state_key, actions_dict in st.session_state.rl_optimizer.q_table.items():
            row_dict = {"SRE Telemetry State": state_key}
            for act in ['force_delete', 'restart', 'scale_up', 'scale_down', 'do_nothing']:
                row_dict[act] = actions_dict.get(act, 0.0)
            q_table_data.append(row_dict)
            
        if q_table_data:
            q_df = pd.DataFrame(q_table_data)
            st.dataframe(q_df, hide_index=True, width='stretch', height=220)
        else:
            sample_states = [
                {"SRE Telemetry State": "water_high_cpu_ok_mem", "force_delete": 8.0, "restart": 6.0, "scale_up": 12.0, "scale_down": -5.0, "do_nothing": 0.0},
                {"SRE Telemetry State": "kitchen_med_cpu_high_mem", "force_delete": 12.0, "restart": 12.0, "scale_up": 4.0, "scale_down": -5.0, "do_nothing": 0.0},
                {"SRE Telemetry State": "traffic_ok_cpu_ok_mem", "force_delete": 10.0, "restart": 8.0, "scale_up": 2.0, "scale_down": 2.0, "do_nothing": 10.0}
            ]
            q_df = pd.DataFrame(sample_states)
            st.dataframe(q_df, hide_index=True, width='stretch', height=180)
            st.info("Optimizer has initialized baseline SRE state vectors.")
            
        st.markdown("---")
        st.markdown("#### 🧪 Autonomous SRE Policy Logs")
        
        # Display history log
        if st.session_state.history_list:
            hist_items = []
            for item in st.session_state.history_list:
                reward_val = item.get("reward", 0)
                reward_str = f"+{reward_val}" if reward_val > 0 else str(reward_val)
                reward_lbl = f"<span style='color: #39ff14; font-weight:bold;'>{reward_str}</span>" if reward_val > 0 else f"<span style='color: #f85149; font-weight:bold;'>{reward_str}</span>" if reward_val < 0 else f"<span style='color: #8b949e;'>{reward_str}</span>"
                
                trigger = item.get("triggeredBy", "MANUAL")
                trigger_lbl = f"<span style='color: #00e5ff;'>{trigger}</span>" if trigger != "MANUAL" else f"<span style='color: #8b949e;'>{trigger}</span>"
                
                hist_items.append(clean_html(f"""
                    <div style="font-family: monospace; font-size: 14px; border-bottom: 1px solid #21262d; padding: 10px 0;">
                        <div style="display: flex; justify-content: space-between;">
                            <span>[{item['timestamp']}] <strong>{item['action'].upper()}</strong> on pod {item['podName']}</span>
                            <span>Reward: {reward_lbl}</span>
                        </div>
                        <div style="margin-top: 4px; color: #8b949e; font-size: 13px;">
                            Message: {item['message']} | Trigger: {trigger_lbl}
                        </div>
                        <div style="margin-top: 3px; color: #58a6ff; font-size: 12px;">
                            {item.get('notes', '')}
                        </div>
                    </div>
                """))
            st.markdown("".join(hist_items), unsafe_allow_html=True)
        else:
            st.info("No policy actions logged in this simulation run.")

    with rl_col2:
        st.markdown("#### Reward Assignment Calibration")
        st.markdown("Define specific weights and penalties dynamically applied to direct optimal agent decisions:")
        
        # Display baseline parameters for user reference
        st.markdown(clean_html("""
            <div style="background-color: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 18px;">
                <h5 style="margin: 0 0 12px 0; font-size: 16px; color: #fff;">SRE Policy Reward Structure:</h5>
                <ul style="margin: 0; padding-left: 20px; font-size: 14px; color: #c9d1d9; line-height: 1.6;">
                    <li><strong>Horizontal Scale Up</strong> in response to traffic spikes: <span style="color:#39ff14;">+12 Reward</span></li>
                    <li><strong>rollout restart / delete pod</strong> in response to memory leaks: <span style="color:#39ff14;">+12 Reward</span></li>
                    <li><strong>Process Termination</strong> in response to rogue stress testing binaries: <span style="color:#39ff14;">+12 Reward</span></li>
                    <li><strong>Ineffective or destructive</strong> remediation steps: <span style="color:#ff3b30;">-5 to -10 Penalty</span></li>
                </ul>
            </div>
        """), unsafe_allow_html=True)
        
        st.markdown("<div style='height: 15px;'></div>", unsafe_allow_html=True)
        
        st.markdown("#### Live Telemetry Metrics")

        st.metric(label="Groq LLM Queries Executed", value=st.session_state.ai_queries_count)
        st.metric(label="Continuous Scrape Step", value=st.session_state.total_steps)

# Footer credit
st.markdown(clean_html("""
    <hr style="border-color: #30363d;" />
    <div style="text-align: center; font-size: 10px; color: #8b949e; font-family: monospace; padding-bottom: 20px;">
        AI-Pod Site Reliability Control Matrix • Engineered fully in Python • Conforms to standard SRE SLA guidelines.
    </div>
"""), unsafe_allow_html=True)

# Live continuous refresh block to auto-run the telemetry scraping loop every 3 seconds
if loop_enabled:
    time.sleep(3)
    st.rerun()

