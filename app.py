# app.py - Complete Working SRE Platform (NO applymap)
import streamlit as st
import pandas as pd
import time
import plotly.graph_objects as go
from datetime import datetime

# Import all modules
from dynamic_discovery import pod_discovery
from kafka_producer import kafka_producer
from kafka_consumer import kafka_consumer
from anomaly_detection import anomaly_detector
from config import config

# Page configuration
st.set_page_config(
    page_title="SRE Platform - ESP Pod Monitor",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        margin: 5px 0;
    }
    .critical-alert {
        background-color: #ff4444;
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
    }
    .warning-alert {
        background-color: #ffa500;
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
    }
    .pod-card {
        background-color: #262730;
        padding: 10px;
        border-radius: 8px;
        margin: 5px;
        border-left: 4px solid;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.title("🚀 Autonomous SRE Platform")
st.caption("Real-time ESP Pod Monitoring | Prometheus → Kafka → Streamlit")

# Initialize session state
if 'services_started' not in st.session_state:
    st.session_state.services_started = False
if 'alerts_history' not in st.session_state:
    st.session_state.alerts_history = []

# Sidebar
with st.sidebar:
    st.header("📊 Platform Status")
    
    # Pod Discovery Status
    st.subheader("🔍 ESP Pod Discovery")
    esp_pods = [k for k in pod_discovery.pods.keys() if 'esp' in k.lower()]
    st.metric("Discovered ESP Pods", len(esp_pods))
    
    if esp_pods:
        for pod_key in esp_pods[:5]:
            pod_name = pod_key.split('/')[-1]
            namespace = pod_key.split('/')[0]
            st.write(f"• **{pod_name}** ({namespace})")
    
    st.divider()
    
    # Kafka Status
    st.subheader("📡 Kafka Stream")
    if hasattr(kafka_producer, 'producer') and kafka_producer.producer:
        st.success("✅ Producer Connected")
    else:
        st.warning("⚠️ Producer Starting...")
    
    if hasattr(kafka_consumer, 'consumer') and kafka_consumer.consumer:
        st.success("✅ Consumer Connected")
        st.metric("Messages", kafka_consumer.message_count)
    else:
        st.warning("⚠️ Consumer Starting...")
    
    st.divider()
    
    # Controls
    st.subheader("🎮 Controls")
    refresh_rate = st.slider("Refresh Rate (seconds)", 2, 10, 3)

# Start services (only once)
if not st.session_state.services_started:
    with st.spinner("🚀 Starting SRE Platform Services..."):
        try:
            kafka_producer.start()
        except Exception as e:
            st.error(f"Producer error: {e}")
        try:
            kafka_consumer.start()
        except Exception as e:
            st.error(f"Consumer error: {e}")
        try:
            pod_discovery.discover_pods()
        except Exception as e:
            st.error(f"Discovery error: {e}")
    st.session_state.services_started = True
    st.success("✅ Services started!")

# Helper function for status
def get_status_emoji(cpu: float, memory_mb: float) -> str:
    if cpu > 0.9 or memory_mb > 1024:
        return "🔴"
    elif cpu > 0.7 or memory_mb > 512:
        return "🟡"
    elif cpu > 0.5:
        return "🟠"
    else:
        return "🟢"

def get_status_text(cpu: float, memory_mb: float) -> str:
    if cpu > 0.9 or memory_mb > 1024:
        return "CRITICAL"
    elif cpu > 0.7 or memory_mb > 512:
        return "WARNING"
    elif cpu > 0.5:
        return "DEGRADED"
    else:
        return "HEALTHY"

# Main layout
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📊 Real-time ESP Pod Metrics")
    metrics_container = st.container()
    chart_container = st.container()

with col2:
    st.subheader("🚨 Alerts")
    alerts_container = st.container()
    st.subheader("🤖 Recommendations")
    recommendations_container = st.container()

# Update function
def update_dashboard():
    metrics = kafka_consumer.get_latest_metrics()
    
    if not metrics:
        with metrics_container:
            st.info("⏳ Waiting for metrics from Kafka...")
            st.caption("This may take 30-60 seconds")
        return 0
    
    # Build data
    records = []
    alerts = []
    high_cpu_pods = []
    
    for pod_key, m in metrics.items():
        pod_name = pod_key.split('/')[-1]
        namespace = m.get('namespace', 'unknown')
        cpu = m.get('cpu', 0)
        memory_mb = m.get('memory_mb', 0)
        
        if cpu == 0 and memory_mb == 0:
            continue
        
        status_emoji = get_status_emoji(cpu, memory_mb)
        status_text = get_status_text(cpu, memory_mb)
        
        records.append({
            "Pod": pod_name,
            "Namespace": namespace,
            "CPU %": f"{cpu*100:.1f}%",
            "Memory (MB)": f"{memory_mb:.0f}",
            "Status": f"{status_emoji} {status_text}",
            "cpu_raw": cpu,
            "severity": status_text.lower()
        })
        
        if cpu > 0.7:
            high_cpu_pods.append((pod_name, namespace, cpu))
        
        if status_text in ["CRITICAL", "WARNING"]:
            alerts.append({
                "pod": pod_name,
                "namespace": namespace,
                "cpu": cpu,
                "memory": memory_mb,
                "severity": status_text,
                "time": datetime.now().strftime("%H:%M:%S")
            })
    
    if not records:
        with metrics_container:
            st.warning("No ESP pod data yet")
        return 0
    
    # Display table
    with metrics_container:
        df = pd.DataFrame(records)
        display_cols = ['Pod', 'Namespace', 'CPU %', 'Memory (MB)', 'Status']
        st.dataframe(df[display_cols], use_container_width=True, hide_index=True)
    
    # Display chart
    with chart_container:
        st.subheader("📈 CPU Usage")
        chart_df = pd.DataFrame([{"Pod": r["Pod"], "CPU %": float(r["CPU %"].replace('%', ''))} for r in records])
        
        if not chart_df.empty:
            fig = go.Figure()
            colors = ['red' if x > 70 else 'orange' if x > 50 else 'green' for x in chart_df["CPU %"]]
            fig.add_trace(go.Bar(x=chart_df["Pod"], y=chart_df["CPU %"], marker_color=colors, text=chart_df["CPU %"], textposition='auto'))
            fig.add_hline(y=70, line_dash="dash", line_color="orange", annotation_text="Warning (70%)")
            fig.add_hline(y=90, line_dash="dash", line_color="red", annotation_text="Critical (90%)")
            fig.update_layout(height=300, title="CPU Usage by Pod")
            st.plotly_chart(fig, use_container_width=True)
    
    # Display alerts
    with alerts_container:
        if alerts:
            for alert in alerts[:3]:
                icon = "🔴" if alert['severity'] == "critical" else "🟡"
                st.markdown(f"""
                <div class="pod-card" style="border-left-color: {'#ff4444' if alert['severity'] == 'critical' else '#ffa500'}">
                    <b>{icon} {alert['pod']}</b><br>
                    <small>CPU: {alert['cpu']*100:.1f}% | Memory: {alert['memory']:.0f}MB</small><br>
                    <small>{alert['time']}</small>
                </div>
                """, unsafe_allow_html=True)
            st.session_state.alerts_history.extend(alerts)
        else:
            st.success("✅ No anomalies")
    
    # Display recommendations
    with recommendations_container:
        if high_cpu_pods:
            st.warning("🔥 High CPU detected")
            for pod_name, namespace, cpu in high_cpu_pods[:2]:
                st.code(f"kubectl scale deployment {pod_name} -n {namespace} --replicas=3", language="bash")
        else:
            st.success("✅ System healthy")
            if st.session_state.alerts_history:
                with st.expander("📜 Alert History"):
                    for alert in st.session_state.alerts_history[-5:]:
                        st.write(f"• {alert['time']}: {alert['pod']} - {alert['severity']}")
    
    return len(records)

# Auto-refresh loop
while True:
    pod_count = update_dashboard()
    if pod_count == 0:
        st.info("🔄 Waiting for ESP pods...")
    time.sleep(refresh_rate)
    st.rerun()
