# SRE Kubernetes Portal: Autonomous Agent & Reinforcement Learning Deck

Welcome to the **Site Reliability Engineering (SRE) Autonomous Agent & Reinforcement Learning Portal**. This project is an advanced, high-fidelity interactive simulation and demonstration desk designed to bridge the gap between traditional SRE observability and **autonomous, self-healing closed-loop orchestration** using Machine Learning and AI.

demo video = https://drive.google.com/file/d/16NTmEjZWg7LVvfEZ0RcxtsehvWcQLkdc/view?usp=sharing
---

## 🏗️ 1. Why this Project Matters: The Role of Kubernetes vs. SRE AI Agents

### What is the "Work" of Kubernetes?
Kubernetes (K8s) is a production-grade container orchestration system. It is designed to automate the deployment, scaling, and management of containerized applications. It focuses on maintaining **infrastructure state** based on static declarative configurations:
* **Scheduling:** Placing containers onto appropriate physical or virtual server nodes.
* **Declarative Scaling:** Ensuring that if you declare `replicas: 3`, there are always 3 instances running.
* **Basic Self-Healing (Liveness/Readiness):** If a process inside a container crashes and exits (non-zero status), K8s restarts the container. If a whole node dies, K8s reschedules the container on another node.

### What is the Point of this Project (SRE AI Agent)?
While Kubernetes handles **infrastructure-level crashes**, it is blind to complex, high-level **application-layer anomalies** or **SLA degradations** that do not crash the container binary. This is where this SRE AI Agent comes into play:

1. **Undetected Anomalies (Gray Failures):** A containerized app can be running perfectly from Kubernetes' perspective (returning code `0` or remaining alive), while in reality, it is suffering from a **gradual memory leak**, **infinite loops thrashing CPU cores**, or **deadlocked request buffers**. K8s does *not* automatically restart these because the container hasn't crashed.
2. **Smart Closed-Loop Remediation:** Instead of waiting for a human SRE to wake up at 3:00 AM, our portal acts as an **autonomous agent** that:
   * Constantly scrapes metric pipelines (using `dynamic_discovery.py`).
   * Detects abnormal behavior using an **Isolation Forest** machine learning model (`anomaly_detection.py`).
   * Fetches advanced diagnostic intelligence via **Groq LLM integrations** (`GroqSREAgent`).
   * Evaluates the best mitigation step (Restart, Horizontal Scaling, Force Delete, Standby, or Do Nothing) using a **Q-Learning Reinforcement Learning Policy** (`rl_optimizer.py`).
   * Executes the exact Kubernetes API operations (`k8s_client.py`) to heal the cluster dynamically, tracking its success rate and updating its intelligence iteratively.

---

## 🧠 2. Machine Learning, AI & Telemetry Deep Dive

### 🌲 A. What is the Isolation Forest Algorithm?
**Isolation Forest** is an unsupervised anomaly detection algorithm based on decision trees. 

#### How It Works:
1. Instead of profiling "normal" data points, it specifically focuses on **isolating** anomalies.
2. It randomly selects a feature (e.g., CPU %) and then randomly selects a split value between the minimum and maximum values of that feature.
3. Recursive partitioning can be represented by a tree structure (Isolation Tree).
4. **The Core Logic:** Anomalies are far from normal clusters and have extreme feature values. Therefore, they require **very few splits** to be isolated. On the other hand, normal points are packed densely and require **many splits** to be isolated.
5. If a data point travels a very short path length from the root to its terminal leaf, it receives a negative anomaly score (closer to `-1.0`), signifying a high-severity anomaly.

In `anomaly_detection.py`, we train an `IsolationForest` on baseline operating metrics (CPU and Memory). When the chaos agent injects stressors, the model isolates these spikes instantly, triggering SRE alerts.

---

### 🤖 B. Smart Diagnostics with Groq AI
The platform integrates with the **Groq API** (`llama-3.3-70b-versatile`) to perform deep-dive diagnostics on ailing pods.
* **Pre-flight Health Checks:** Before burning API credits, the `GroqSREAgent` runs an internal health check. If the pod is completely healthy (normal CPU/Mem, no anomalies, no stressful internal processes), the agent automatically advises a `do_nothing` action.
* **LLM Remediation Mapping:** When anomalies *are* detected, Groq analyzes the raw telemetry and `ps aux` internal process states to formulate highly technical, ranked `kubectl` intervention strategies.

---

### 🕹️ C. Where is the Q-Learning Table Used?
The Reinforcement Learning **Q-Table** is the decision-making brain of our autonomous agent. 
* **Persistent Storage (SQLite):** Unlike basic memory arrays, the agent's intelligence is securely stored in a local SQLite database (`sre_agent.db`). This allows the agent to continuously learn and maintain its Q-table state across server reboots.
* **The Brain (`rl_optimizer.py`):** It takes a pod's current state (e.g., `traffic_high_cpu_ok_mem`), retrieves Q-values from SQLite, and dynamically ranks the LLM's remediation suggestions based on historical success rates.
* **Interactive Explorer (`app.py`):** Inside the **"🧠 RL Q-Learning Policy"** tab, SRE operators can view a live tabular DataFrame representing the exact raw values of the Q-Table. Watch as the agent learns through feedback loops, accumulating positive rewards for optimal actions (like scaling up during traffic) and penalties for disruptive ones.

---

## 📂 3. Repository Architecture & File Blueprint

Here is the finalized operational blueprint of the autonomous cluster environment:

```bash
├── app.py                     # Main Streamlit Portal: operator dashboard, visualizer, metrics charts
├── dynamic_discovery.py       # Live Scraper: discovers real running docker containers dynamically
├── k8s_client.py              # Stateful K8s Simulator: handles container scaling & force deletions
├── rl_optimizer.py            # RL Engine & Groq AI: Q-table updates, SQLite sync, and LLM diagnostics
├── anomaly_detection.py       # ML Pipeline: runs Isolation Forest to identify anomalous spikes
├── config.py                  # Core Configurations: API Keys, RL rates, and Kafka topics
├── sre_agent.db               # SQLite DB: Persistent storage for the SRE Q-Learning Table
├── esp_deployments.yaml       # K8s Deployment Manifests for simulated topologies
└── requirements.txt           # Python package dependencies
```

---

## 🔒 4. Secure Operator Login & Setup

The SRE portal is protected by a cryptographically styled access gate.

### Default Credentials:
* **Username / SRE Operator ID:** `admin`
* **Access Control Password:** `sre-password`

### Running the Project:
1. Ensure your API keys (like `GROQ_API_KEY`) are set in `config.py` or your environment.
2. Install dependencies: `pip install -r requirements.txt`
3. Launch the portal: `streamlit run app.py`

Once authenticated, the console immediately decrypts the active Kafka telemetry stream, boots the live metrics loops, and begins active anomaly scanning.
