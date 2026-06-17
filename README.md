# 🚀 Autonomous SRE Platform

AI-powered Site Reliability Engineering platform that automatically detects, analyzes, and remediates Kubernetes pod anomalies using Machine Learning and Reinforcement Learning.

## Overview

This platform combines:

* 🔍 Dynamic Kubernetes pod discovery
* 📊 Real-time metrics monitoring with Prometheus
* ⚡ Kafka-based event streaming
* 🤖 ML-driven anomaly detection (Isolation Forest)
* 🧠 Reinforcement Learning for remediation optimization
* 💡 LLM-assisted remediation suggestions (Groq Llama 3.1)
* 📈 Interactive Streamlit dashboard

---

## Architecture

```text
Kubernetes Pods
      │
      ▼
 Prometheus
      │
      ▼
Kafka Producer
      │
      ▼
 Kafka Topic
      │
      ▼
Kafka Consumer
      │
      ▼
Streamlit Dashboard
      │
      ▼
Anomaly Detection
      │
      ▼
RL-Based Remediation
      │
      ▼
kubectl Actions
```

---

## Key Features

* Dynamic pod discovery
* Real-time metrics pipeline
* Isolation Forest anomaly detection
* Q-Learning remediation engine
* AI-generated remediation recommendations
* Interactive monitoring dashboard
* Automated or manual remediation workflows
* Learning-based action optimization

---

## Tech Stack

| Layer                  | Technology        |
| ---------------------- | ----------------- |
| Orchestration          | Kubernetes (Kind) |
| Monitoring             | Prometheus        |
| Streaming              | Apache Kafka      |
| Machine Learning       | scikit-learn      |
| Reinforcement Learning | Q-Learning        |
| LLM                    | Groq Llama 3.1    |
| Dashboard              | Streamlit         |
| Storage                | SQLite            |
| Visualization          | Plotly            |

---

## Project Structure

```text
ai-pod/
├── app.py
├── config.py
├── models.py
├── anomaly_detection.py
├── dynamic_discovery.py
├── kafka_producer.py
├── kafka_consumer.py
├── rl_optimizer.py
├── docker-compose-kafka.yml
├── esp_deployments_fixed.yaml
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Clone Repository

```bash
git clone <repo-url>
cd ai-pod

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Create Kubernetes Cluster

```bash
kind create cluster --name sre
```

### 3. Deploy Sample Workloads

```bash
kubectl apply -f esp_deployments_fixed.yaml
```

### 4. Install Prometheus

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm install prometheus prometheus-community/prometheus

kubectl port-forward svc/prometheus-server 9090:80 &
```

### 5. Start Kafka

```bash
docker compose -f docker-compose-kafka.yml up -d
```

### 6. Run Dashboard

```bash
export GROQ_API_KEY=<your-api-key>

streamlit run app.py
```

Open:

```text
http://localhost:8501
```

---

## How It Works

### Metrics Pipeline

```text
Pods → Prometheus → Kafka → Streamlit
```

### Anomaly Detection

* Metrics are analyzed using Isolation Forest.
* Abnormal resource patterns are flagged as anomalies.
* Alerts are displayed in real time.

### Reinforcement Learning

The RL agent learns the best remediation action based on cluster state.

**Actions**

* Scale Up
* Scale Down
* Restart Pod
* Do Nothing

**Rewards**

* Successful recovery → Positive reward
* Failed remediation → Negative reward

Over time, the agent improves remediation decisions.

---

## Configuration

Update values in `config.py`:

```python
prometheus_url = "http://localhost:9090"
kafka_bootstrap_servers = "localhost:9092"
refresh_interval = 5

rl_learning_rate = 0.1
rl_discount_factor = 0.95
rl_epsilon = 0.1
```

---

## Troubleshooting

### Kafka Issues

```bash
docker compose -f docker-compose-kafka.yml restart
docker logs kafka
```

### Prometheus Unreachable

```bash
kubectl port-forward svc/prometheus-server 9090:80 &
```

### Verify Workloads

```bash
kubectl get pods --all-namespaces
```

---

Built for autonomous Kubernetes operations and intelligent SRE workflows. 🚀
