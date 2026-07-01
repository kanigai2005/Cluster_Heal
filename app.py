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
from rl_optimizer import QLearningOptimizer, GeminiSREAgent, ACTIONS
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
if 'gemini_agent' not in st.session_state:
    st.session_state.gemini_agent = GeminiSREAgent()
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
    
    # Efficiently gather stats for real containers and processes in one go
    docker_stats = st.session_state.discovery_engine.scrape_docker_stats() if st.session_state.discovery_engine.is_docker_available else {}
    system_procs = {p["pid"]: p for p in st.session_state.discovery_engine.scrape_system_processes()} if not docker_stats else {}
    
    for pod in st.session_state.pods:
        if pod.get("is_real", False):
            # 1. Update from actual scraped live metrics
            if pod.get("type") == "docker":
                c_stats = docker_stats.get(pod["name"], None)
                if c_stats:
                    pod["cpu_pct"] = c_stats["cpu_pct"]
                    pod["memory_mb"] = c_stats["memory_mb"]
                    pod["memory_limit"] = max(1.0, c_stats["memory_limit"])
            elif pod.get("type") == "process":
                proc_info = system_procs.get(pod.get("pid"), None)
                if proc_info:
                    pod["cpu_pct"] = min(100.0, proc_info["cpu"])
                    # Translate process memory % of standard host system to MB (assuming 4GB)
                    pod["memory_mb"] = round((proc_info["mem"] / 100.0) * 4096.0, 1)
            
            # Recalculate Cores cpu usage from CPU %
            pod["cpu"] = round((pod["cpu_pct"] / 100.0) * pod["cpu_limit"], 2)
            
            # Status check
            mem_pct = (pod["memory_mb"] / max(1.0, pod["memory_limit"])) * 100.0
            if pod["cpu_pct"] >= config.CPU_CRITICAL_THRESHOLD_PCT or mem_pct >= config.MEM_CRITICAL_THRESHOLD_PCT:
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
            continue

        # 2. Simulated fluctuation for simulation containers
        # Check active processes to decide baseline metrics
        has_stress = any("stress" in p["command"] for p in pod.get("activeProcesses", []))
        has_leak = any("java" in p["command"] for p in pod.get("activeProcesses", []))
        
        target_cpu_pct = 15.0
        target_mem_pct = 20.0
        
        if has_stress:
            target_cpu_pct = 94.0
        if has_leak:
            # Memory leak grows based on how long pod has been alive (creationTime)
            elapsed_sec = (time.time() - (pod["creationTime"] / 1000.0))
            # Grows 0.5% per minute
            leak_addition = min(75.0, (elapsed_sec / 60.0) * 8.0)
            target_mem_pct = 40.0 + leak_addition
            target_cpu_pct = max(target_cpu_pct, 45.0)
            
        # Replicas divide the CPU stress
        if pod["replicas"] > 1:
            target_cpu_pct = target_cpu_pct / pod["replicas"]
            target_mem_pct = target_mem_pct / (pod["replicas"] * 0.9)
            
        # Fluctuate with smooth average
        pod["cpu_pct"] = max(1.0, min(100.0, pod["cpu_pct"] * 0.8 + target_cpu_pct * 0.2 + random.uniform(-1.5, 1.5)))
        pod["cpu"] = round((pod["cpu_pct"] / 100.0) * pod["cpu_limit"], 2)
        
        # Calculate memory MB
        max_mem = pod["memory_limit"]
        target_mb = (target_mem_pct / 100.0) * max_mem
        pod["memory_mb"] = max(10.0, min(max_mem, pod["memory_mb"] * 0.9 + target_mb * 0.1 + random.uniform(-3.0, 3.0)))
        pod["memory_mb"] = round(pod["memory_mb"], 1)
        
        # Sync active process logs
        for p in pod.get("activeProcesses", []):
            if "stress" in p["command"]:
                p["cpu"] = round(pod["cpu_pct"] * 0.85 + random.uniform(-0.5, 0.5), 1)
            elif "java" in p["command"]:
                p["mem"] = round((pod["memory_mb"] / max_mem) * 100.0 * 0.75 + random.uniform(-0.5, 0.5), 1)
                p["cpu"] = round(25.0 + random.uniform(-2.0, 2.0), 1)

        # Status check
        mem_pct = (pod["memory_mb"] / max(1.0, pod["memory_limit"])) * 100.0
        if pod["cpu_pct"] >= config.CPU_CRITICAL_THRESHOLD_PCT or mem_pct >= config.MEM_CRITICAL_THRESHOLD_PCT:
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

    # Simulate Kafka telemetry stream push
    pushed_logs = st.session_state.kafka_producer.send_telemetry_batch(st.session_state.pods, config.KAFKA_TOPIC_TELEMETRY)
    st.session_state.kafka_msg_log.extend(pushed_logs)
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
            for act in ['scale_up', 'scale_down', 'restart', 'delete_pod', 'kill_process', 'do_nothing']:
                if act == 'scale_up' and target_pod["replicas"] >= 3:
                    continue
                if act == 'scale_down' and target_pod["replicas"] <= 1:
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
    pod = next((p for p in st.session_state.pods if p["name"] == pod_name), None)
    if not pod:
        st.toast(f"Error: Target pod {pod_name} not found.", icon="⚠️")
        return
        
    prev_state_key = st.session_state.rl_optimizer.get_state_key(pod)
    message = ""
    success = True
    
    # 1. Print simulated Kubernetes API server log
    api_log = ""
    if action == "scale_up":
        api_log = st.session_state.k8s_client.scale_deployment(pod["deployment"], 3, pod["namespace"])
        message = f"Scaled deployment '{pod['deployment']}' horizontally to 3 replicas. Load balanced successfully."
    elif action == "scale_down":
        api_log = st.session_state.k8s_client.scale_deployment(pod["deployment"], 1, pod["namespace"])
        message = f"Scaled deployment '{pod['deployment']}' down to 1 baseline replica."
    elif action == "restart":
        api_log = st.session_state.k8s_client.rollout_restart(pod["deployment"], pod["namespace"])
        message = f"rollout restart completed for {pod['deployment']}. Active thread buffers flushed."
    elif action == "delete_pod":
        old_name = pod["name"]
        api_log = st.session_state.k8s_client.delete_pod(old_name, pod["namespace"])
        message = f"Terminated old pod {old_name}. K8s replica controller provisioned replacement instance."
    elif action == "kill_process":
        # Find this pod inside k8s_client
        k8s_pod = next((p for p in st.session_state.k8s_client.pods if p["name"] == pod["name"]), None)
        if "smart-kitchen" in pod["deployment"]:
            api_log = st.session_state.k8s_client.exec_container_kill(pod["name"], pod["namespace"], "kitchen-hub", "java")
            message = f"Sent SIGKILL to PID 1 java binary in {pod['name']}. Triggered immediate container panic rollout."
        elif k8s_pod and any("stress" in p["command"] for p in k8s_pod.get("activeProcesses", [])):
            api_log = st.session_state.k8s_client.exec_container_kill(pod["name"], pod["namespace"], "stress-agent", "stress")
            message = "Terminated rogue stress tool execution thread inside the container. Core CPU levels returned to normal."
        else:
            api_log = f"Exec shell: process 'stress' not found in pod {pod['name']}."
            message = "Remediation action had no effect; no rogue stress tool runs detected."
            success = False
    elif action == "do_nothing":
        api_log = "API standby. Telemetry check only."
        message = "Standing by. System continued alerting cycles."

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
        has_stress = any("stress" in p["command"] for p in pod.get("activeProcesses", []))
        
        if cpu_val > 70.0 or has_stress:
            # CPU Issue or stress binary running
            if action == "kill_process":
                reward = 12
                message += " [SRE OPTIMIZED] Process stressor safely terminated!"
            elif action == "scale_up":
                reward = 6
                message += " [SRE OK] Scaled out replicas to cushion load."
            elif action in ["restart", "delete_pod"]:
                reward = 4
                message += " [SRE AGGRESSIVE] Pod restarted to shed thread locks."
            else:
                reward = -5
        elif mem_val > 80.0:
            # Memory leakage issue
            if action in ["restart", "delete_pod"]:
                reward = 12
                message += " [SRE OPTIMIZED] Recycled pod memory footprint."
            elif action == "scale_up":
                reward = 3
                message += " [SRE BUFFER] Scale up added temporary memory overhead."
            else:
                reward = -5
        else:
            # Nominal, healthy state
            if action == "do_nothing":
                reward = 12
            else:
                reward = -5  # Penalize disruptive or wasteful remediation on healthy nodes
    else:
        reward = -5

    # Update Q-Learning weights table
    next_state_key = st.session_state.rl_optimizer.get_state_key(pod)
    new_q = st.session_state.rl_optimizer.update_q_value(prev_state_key, action, reward, next_state_key)
    
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
    st.session_state.rl_optimizer.q_table = {}
    st.session_state.ai_recs_cache = {}
    st.session_state.kafka_msg_log = []
    
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
    st.markdown("### 📊 Policy Hyperparameters")
    st.session_state.epsilon = st.slider("Exploration Rate (ε)", 0.0, 1.0, float(st.session_state.epsilon), 0.05)
    st.markdown(f"**Discount Factor (γ):** `{config.RL_DISCOUNT_FACTOR}`")
    st.markdown(f"**Learning Rate (α):** `{config.RL_LEARNING_RATE}`")
    st.markdown(f"**Scraped Telemetry Count:** `{st.session_state.total_steps}`")
    
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
                <p style="margin: 4px 0 0 0; font-size: 12px; color: #8b949e;">Kubernetes Namespace Control Deck • Reinforcement Learning Optimization Engine • Gemini 2.5 Diagnostic Copilot</p>
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
            if pod["status"] == "CRITICAL":
                card_style = "pod-card-critical"
            elif pod["status"] == "WARNING":
                card_style = "pod-card-warning"
                
            status_color = "#39ff14" if pod["status"] == "HEALTHY" else "#d29922" if pod["status"] == "WARNING" else "#f85149"
            anomaly_lbl = f"<span style='color: #f85149; font-weight:bold;'>Anomaly ({pod['anomalyScore']})</span>" if pod["isAnomaly"] else f"<span style='color: #39ff14;'>Nominal ({pod['anomalyScore']})</span>"
            
            st.markdown(clean_html(f"""
                <div class="{card_style}">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                        <span style="font-size: 13px; background-color: #21262d; border: 1px solid #30363d; padding: 4px 10px; border-radius: 6px; font-family: monospace; color: #8b949e;">{pod['namespace']}</span>
                        <strong style="color: {status_color}; font-size: 13px; text-transform: uppercase; border: 1px solid {status_color}50; padding: 3px 10px; border-radius: 12px; background-color: {status_color}10;">{pod['status']}</strong>
                    </div>
                    <div style="margin-bottom: 16px; height: 65px; overflow: hidden;">
                        <h4 style="margin: 0; font-size: 17px; color: #f0f6fc; text-overflow: ellipsis; white-space: nowrap;" title="{pod['name']}">{pod['name']}</h4>
                        <span style="font-size: 13px; color: #8b949e;">Deployment: <strong>{pod['deployment']}</strong></span>
                    </div>
                    
                    <!-- CPU Metric Gauge -->
                    <div style="margin-bottom: 14px;">
                        <div style="display: flex; justify-content: space-between; font-size: 13px; font-family: monospace; margin-bottom: 4px;">
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
                        <div style="display: flex; justify-content: space-between; font-size: 13px; font-family: monospace; margin-bottom: 4px;">
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
                    <div style="display: flex; gap: 8px; margin-bottom: 14px; background-color: #0d1117; padding: 8px; border-radius: 8px; border: 1px solid #21262d; justify-content: space-between; text-align: center;">
                        <div style="flex:1;">
                            <span style="font-size: 11px; color: #8b949e; display:block; font-family: monospace; font-weight: bold;">REPLICAS</span>
                            <strong style="font-size: 14px; color: #c9d1d9; font-family: monospace;">{pod['replicas']}/3</strong>
                        </div>
                        <div style="flex:1; border-left: 1px solid #21262d; border-right: 1px solid #21262d;">
                            <span style="font-size: 11px; color: #8b949e; display:block; font-family: monospace; font-weight: bold;">RESTARTS</span>
                            <strong style="font-size: 14px; color: #c9d1d9; font-family: monospace;">{pod['restarts']}</strong>
                        </div>
                        <div style="flex:1;">
                            <span style="font-size: 11px; color: #8b949e; display:block; font-family: monospace; font-weight: bold;">STATUS</span>
                            <strong style="font-size: 14px; color: #c9d1d9; font-family: monospace;">{pod['status'][:4]}</strong>
                        </div>
                    </div>

                    <!-- Isolation score -->
                    <div style="display: flex; justify-content: space-between; align-items: center; font-size: 12px; font-family: monospace; border-top: 1px solid #21262d; padding-top: 8px;">
                        <span style="color: #8b949e;">Isolation Forest:</span>
                        <strong>{anomaly_lbl}</strong>
                    </div>
                </div>
            """), unsafe_allow_html=True)
            
            # Button layouts
            b_col1, b_col2 = st.columns(2)
            with b_col1:
                if pod["status"] == "HEALTHY":
                    if st.button("🔥 Stress Load", key=f"t-stress-{pod['name']}", help="Inject heavy stress loads"):
                        inject_chaos_load(pod["name"])
                        st.rerun()
                else:
                    if st.button("⚡ Quick Heal", key=f"t-heal-{pod['name']}", help="Trigger rapid container restart"):
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
            st.plotly_chart(fig, use_container_width=True)

    with vis_col2:
        st.markdown("### 🧬 active Container Process Monitor (ps aux)")
        diag_pod = next((p for p in st.session_state.pods if p["name"] == chosen_graph_pod_name), None)
        if diag_pod:
            processes = diag_pod.get("activeProcesses", [])
            p_df = pd.DataFrame(processes)
            if not p_df.empty:
                p_df.columns = ["PID", "User", "CPU %", "Mem %", "Command"]
                st.dataframe(p_df, hide_index=True, use_container_width=True, height=220)
            else:
                st.info("No active processes detected in this container core.")
                
    st.markdown("---")
    st.markdown("### 📻 Real-time Telemetry Event Log (Kafka Message Stream)")
    
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
        st.markdown("### 🚨 Active Cluster Alerts & Incidents")
        alert_pods = [p for p in st.session_state.pods if p["status"] != "HEALTHY"]
        
        if alert_pods:
            for p in alert_pods:
                color_theme = "red" if p["status"] == "CRITICAL" else "orange"
                bg_col = "#2d161a" if p["status"] == "CRITICAL" else "#251c10"
                border_col = "#f85149" if p["status"] == "CRITICAL" else "#d29922"
                
                st.markdown(clean_html(f"""
                    <div style="background-color: {bg_col}; border: 1px solid {border_col}; padding: 18px; border-radius: 10px; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="font-size: 14px; background-color: rgba(255,255,255,0.07); padding: 4px 8px; border-radius: 6px; font-family: monospace;">{p['namespace']}</span>
                            <strong style="margin-left: 8px; font-size: 16px; color: #fff; font-family: monospace;">{p['name']}</strong>
                            <div style="margin-top: 8px; font-size: 14px; color: #ffbc5b;">
                                WARNING: {p['deployment']} metrics out of normal bounds. CPU: {p['cpu_pct']:.1f}% | Memory: {p['memory_mb']:.1f} MB
                            </div>
                        </div>
                        <div style="font-size: 14px; text-transform: uppercase; font-family: monospace; font-weight: bold; border: 1px solid {border_col}; padding: 4px 10px; border-radius: 6px; background-color: rgba(0,0,0,0.25);">
                            {p['status']}
                        </div>
                    </div>
                """), unsafe_allow_html=True)
        else:
            st.success("🎉 Excellent! All container pods discovered are currently HEALTHY and operating nominal.")
            
        st.markdown("---")
        st.markdown("### 🛠️ Interactive Remediation Execution Matrix")
        
        target_remedy_pod_name = st.selectbox(
            "Select K8s Pod to Target for Manual Policy Action:",
            options=[p["name"] for p in st.session_state.pods],
            key="remedy_target_selector"
        )
        
        target_remedy_pod = next((p for p in st.session_state.pods if p["name"] == target_remedy_pod_name), None)
        if target_remedy_pod:
            st.markdown(f"**Target Pod Specs:** `{target_remedy_pod['name']}` • Namespace: `{target_remedy_pod['namespace']}` • Deployment: `{target_remedy_pod['deployment']}`")
            
            # Action button matrix grid
            act_cols = st.columns(3)
            with act_cols[0]:
                if st.button("⚡ Horizontal Scale (Up 3x)", use_container_width=True, help="Scale deployment up to 3 replicas"):
                    execute_remediation(target_remedy_pod_name, "scale_up")
                    st.rerun()
                if st.button("🔌 rollout restart", use_container_width=True, help="Re-roll active pod instances"):
                    execute_remediation(target_remedy_pod_name, "restart")
                    st.rerun()
            with act_cols[1]:
                if st.button("📉 Horizontal Scale (Down 1x)", use_container_width=True, help="Scale back down to 1 instance"):
                    execute_remediation(target_remedy_pod_name, "scale_down")
                    st.rerun()
                if st.button("🗑️ Force Delete Pod", use_container_width=True, help="Immediately delete pod instance"):
                    execute_remediation(target_remedy_pod_name, "delete_pod")
                    st.rerun()
            with act_cols[2]:
                if st.button("☠️ Kill stress/java", use_container_width=True, help="Force kill specific stress/java execution binaries"):
                    execute_remediation(target_remedy_pod_name, "kill_process")
                    st.rerun()
                if st.button("🤷 Standing By (Do Nothing)", use_container_width=True, help="Acknowledge alert but stand by"):
                    execute_remediation(target_remedy_pod_name, "do_nothing")
                    st.rerun()

    with rem_col2:
        st.markdown("### 🧠 SRE Diagnostic Advisor (Gemini 2.5 Flash)")
        st.markdown("AI-Copilot reviews active cAdvisor telemetry arrays and container process profiles to produce exact remedial commands.")
        
        ai_target_pod_name = st.selectbox(
            "Select targeted container pod to retrieve AI recipe:",
            options=[p["name"] for p in st.session_state.pods],
            key="ai_target_selector"
        )
        ai_target_pod = next((p for p in st.session_state.pods if p["name"] == ai_target_pod_name), None)
        
        if ai_target_pod:
            if st.button("🔮 Ask Gemini Copilot", help="Query Gemini SRE diagnostic API"):
                with st.spinner("Analyzing container telemetry logs, CPU metrics, memory blocks, and active process matrices..."):
                    # Pull recommendations
                    recs = st.session_state.gemini_agent.get_recommendations(ai_target_pod)
                    st.session_state.ai_recs_cache[ai_target_pod["name"]] = recs
                    st.session_state.ai_queries_count += 1
                    
            # Pull from cache if exists
            cached_recs = st.session_state.ai_recs_cache.get(ai_target_pod["name"], None)
            if cached_recs:
                st.markdown("#### Structured Remediation Prescriptions:")
                for rec in cached_recs:
                    rank = rec.get("rank", 1)
                    action = rec.get("action", "restart").replace("_", " ").upper()
                    impact = rec.get("impact", "medium").upper()
                    reason = rec.get("reason", "")
                    cmd = rec.get("kubectl_command", "")
                    
                    impact_color = "#f85149" if impact == "HIGH" else "#d29922" if impact == "MEDIUM" else "#58a6ff"
                    
                    st.markdown(clean_html(f"""
                        <div style="background-color: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 18px; margin-bottom: 14px;">
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                                <strong style="color: #00e5ff; font-family: monospace; font-size: 16px;">#{rank} REMEDY: {action}</strong>
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
                    raw_act = rec.get("action", "do_nothing")
                    if st.button(f"Apply Remedy #{rank}: {raw_act.replace('_', ' ').title()}", key=f"apply-{rank}-{ai_target_pod['name']}"):
                        execute_remediation(ai_target_pod["name"], raw_act, "AI_COPILOT")
                        st.rerun()
            else:
                st.info("No active diagnostic recipe generated for this pod. Query the Gemini Advisor above to generate recipes.")

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
            for act in ACTIONS:
                row_dict[act] = actions_dict.get(act, 0.0)
            q_table_data.append(row_dict)
            
        if q_table_data:
            q_df = pd.DataFrame(q_table_data)
            st.dataframe(q_df, hide_index=True, use_container_width=True, height=220)
        else:
            # Seed state visual table
            sample_states = [
                {"SRE Telemetry State": "traffic_high_cpu_ok_mem", "scale_up": 12.0, "scale_down": -5.0, "restart": -2.0, "delete_pod": -2.0, "kill_process": -10.0, "do_nothing": -5.0},
                {"SRE Telemetry State": "kitchen_med_cpu_high_mem", "scale_up": -3.0, "scale_down": -5.0, "restart": 10.0, "delete_pod": 10.0, "kill_process": 2.0, "do_nothing": -5.0},
                {"SRE Telemetry State": "water_high_cpu_ok_mem", "scale_up": -4.0, "scale_down": -5.0, "restart": 4.0, "delete_pod": 4.0, "kill_process": 12.0, "do_nothing": -5.0}
            ]
            q_df = pd.DataFrame(sample_states)
            st.dataframe(q_df, hide_index=True, use_container_width=True, height=180)
            st.info("Optimizer has not learned custom states in this session yet. System is using baseline SRE heuristics.")
            
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
        st.metric(label="Exploration Rate (Epsilon Decayed)", value=f"{st.session_state.epsilon:.3f}")
        st.metric(label="Gemini LLM Queries Executed", value=st.session_state.ai_queries_count)
        st.metric(label="Continuous Scrape Step", value=st.session_state.total_steps)

# Footer credit
st.markdown(clean_html("""
    <hr style="border-color: #30363d;" />
    <div style="text-align: center; font-size: 10px; color: #8b949e; font-family: monospace; padding-bottom: 20px;">
        AI-Pod Site Reliability Control Matrix • Engineered fully in Python • Conforms to standard SRE SLA guidelines.
    </div>
"""), unsafe_allow_html=True)
