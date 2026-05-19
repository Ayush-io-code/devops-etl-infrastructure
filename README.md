# DevOps ETL Infrastructure Lab
### Ayush Pareek · Bengaluru · 2026

End-to-end DevOps project replicating enterprise ETL infrastructure using modern open-source tooling.
Built on GitHub Codespaces (Ubuntu 24.04) — containerised, automated, monitored, and CI/CD deployed.

[![CI Pipeline](https://github.com/Ayush-io-code/devops-etl-infrastructure/actions/workflows/ci.yml/badge.svg)](https://github.com/Ayush-io-code/devops-etl-infrastructure/actions)

---

## Why This Project Exists

In my current role I manage **Ab Initio ETL infrastructure** on physical servers — installation,
configuration, Tomcat service reliability, and daily incident coordination with data engineering,
application, and Hive teams. This lab replicates those same patterns using cloud-native open-source
tools, making my operational experience defensible in DevOps and SRE interviews.

| Real Work at LTM | This Lab Equivalent |
|---|---|
| Ab Initio Co>Op job scheduling | Apache Airflow DAGs |
| Manual server provisioning | Ansible playbooks |
| AppDynamics APM monitoring | Prometheus + Grafana |
| Tomcat service management | Docker containerisation |
| Jenkins pipelines | GitHub Actions CI/CD |

---

## Architecture

```
+-----------------------------------------------------+
|               GitHub Actions CI/CD                   |
|       build -> test -> docker validate on push       |
+------------------------+----------------------------+
                         |
            +------------v-------------+
            |       Docker Compose      |
            |                           |
            |  +--------------------+  |
            |  |   Apache Airflow   |  |
            |  |   Airflow 3.x      |  |
            |  |   ETL Pipelines    |  |
            |  |   extract->        |  |
            |  |   transform->load  |  |
            |  +--------------------+  |
            |                           |
            |  +--------------------+  |
            |  |   Prometheus       |  |
            |  |   Grafana          |  |
            |  |   Node Exporter    |  |
            |  +--------------------+  |
            +------------^-------------+
                         |
            +------------+--------------+
            |         Ansible            |
            |   idempotent playbooks     |
            |   automated provisioning   |
            +----------------------------+
```

---

## Weekly Progress

| Week | Tool | What Was Built | Status |
|------|------|----------------|--------|
| 1 | Docker | Flask ETL status app, optimised Dockerfile with layer caching | Done |
| 2 | Apache Airflow | ETL DAG (extract->transform->load), Weather ETL DAG, Docker Compose stack | Done |
| 3 | Ansible | System facts, Docker provisioning, Flask + Airflow deployment playbooks | Done |
| 4 | Prometheus + Grafana | Monitoring stack, PromQL dashboards, alerting | Week 4 |
| 5 | GitHub Actions | CI/CD pipeline — build, test, Docker health check on every push | Week 5 |
| 6 | Portfolio | Full docs, architecture diagrams, interview prep | Week 6 |

---

## Repository Structure

```
devops-etl-infrastructure/
├── README.md
├── .gitignore
│
├── week1-docker/
│   └── flask-app/
│       ├── app.py                      # Flask ETL status service
│       ├── Dockerfile                  # Layer-optimised image
│       └── requirements.txt
│
├── week2-airflow/
│   ├── docker-compose.yaml             # Full Airflow 3.x stack
│   ├── .env.example                    # Environment template
│   ├── config/
│   │   └── airflow.cfg                 # Airflow configuration
│   └── dags/
│       ├── etl_pipeline.py             # Extract -> Transform -> Load DAG
│       └── weather_etl_pipeline.py     # Weather data ETL DAG
│
├── week3-ansible/
│   ├── inventory.ini                   # Host inventory (local connection)
│   ├── system_info.yml                 # Display system facts
│   ├── install_docker.yml              # Provision Docker on Ubuntu
│   ├── deploy_flask.yml                # Build and run Flask container
│   └── deploy_airflow.yml              # Deploy full Airflow stack
│
├── week4-monitoring/                   # Coming Week 4
│   ├── prometheus.yml
│   └── docker-compose.yml
│
└── .github/
    └── workflows/
        └── ci.yml                      # GitHub Actions pipeline (Week 5)
```

---

## How to Run

### Prerequisites
```bash
docker --version
docker compose version
sudo apt install -y ansible
ansible --version
```

### Week 1 — Flask App
```bash
cd week1-docker/flask-app
docker build -t flask-etl-app .
docker run -d -p 5000:5000 --name flask-etl flask-etl-app
curl http://localhost:5000
curl http://localhost:5000/health
```

### Week 2 — Airflow ETL Stack
```bash
cd week2-airflow
echo "AIRFLOW_UID=$(id -u)" > .env
python3 -c "from cryptography.fernet import Fernet; print('FERNET_KEY=' + Fernet.generate_key().decode())" >> .env
docker compose up airflow-init
docker compose up -d
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Access UI at `http://localhost:8080` — login: `airflow` / `airflow`

Trigger `etl_pipeline` DAG from the UI — Graph tab — watch extract->transform->load go green.

> **Airflow 3.x notes:**
> - Use `schedule='@daily'` not `schedule_interval`
> - Import from `airflow.providers.standard.operators.python`

### Week 3 — Ansible Playbooks
```bash
cd week3-ansible

# See system facts (no sudo needed)
ansible-playbook -i inventory.ini system_info.yml

# Provision Docker
ansible-playbook -i inventory.ini install_docker.yml

# Deploy Flask app
ansible-playbook -i inventory.ini deploy_flask.yml

# Deploy Airflow stack
ansible-playbook -i inventory.ini deploy_airflow.yml

# Run again to observe idempotency — changed count drops to near 0
ansible-playbook -i inventory.ini deploy_flask.yml
```

---

## Key Concepts Demonstrated

### Docker
- Layer-optimised Dockerfile — `COPY requirements.txt` before `COPY app.py` so pip cache survives code changes
- Port mapping, container lifecycle, `docker exec` for debugging
- Difference between `EXPOSE` (documentation only) and `-p` (actual host port binding)

### Apache Airflow 3.x
- DAG dependencies with `>>` operator — Python operator overloading
- XCom for inter-task data — `xcom_pull(task_ids='extract')` fetches upstream return values
- `catchup=False` prevents 500+ backfill runs since start_date is 2024
- Architecture: apiserver (uvicorn) serves UI, scheduler parses DAGs, worker executes, Redis queues tasks, PostgreSQL stores metadata

### Ansible
- Agentless — SSH only, no agent required on target servers
- Idempotent — `state: present` checks before acting, safe to re-run in production
- Ansible facts — auto-collected variables like `ansible_distribution`, `ansible_memtotal_mb`
- `ok` vs `changed` in PLAY RECAP — the key indicator of whether state was modified
- `ignore_errors: yes` for expected failures — stop-remove-redeploy container pattern

### Prometheus + Grafana (Week 4)
- Pull model — Prometheus scrapes targets every 15s
- Counter vs Gauge — use `rate()` on counters (cpu_seconds), never on gauges (memory_free)
- Node Exporter exposes Linux system metrics as HTTP endpoints

### GitHub Actions (Week 5)
- Runners — fresh Ubuntu VMs per pipeline run, destroyed after
- `curl -f` for health checks — fails pipeline correctly on HTTP error responses
- Secrets injected as env vars, never visible in logs

---

## Real Debugging Log

Problems hit during this lab and what they taught me:

**Port conflict (Week 2):** Nginx container from Week 1 held port 8080 when Airflow tried to map it. Taught me to always check `docker ps --format "table {{.Names}}\t{{.Ports}}"` before starting new stacks.

**Airflow 3.x breaking change (Week 2):** `schedule_interval` renamed to `schedule`. DAG silently disappears from UI on import error — no loud failure. Fixed with `sed`. Taught me why DAG import error alerting matters in production.

**Heredoc failures (Week 2-3):** Bash interpreted Python's `t1 >> t2 >> t3` as shell redirects when heredoc didn't close properly, creating empty files named `t2` and `t3`. Same symbol, completely different meaning in Python vs bash.

**Package managers (Week 1 vs Codespaces):** `yum` on CentOS, `apt` on Ubuntu. Exactly why Docker and Ansible exist — abstract away OS differences.

---

## Release History

| Tag | Description |
|-----|-------------|
| `v0.3-week3` | Week 3: Ansible playbooks for Docker and Airflow provisioning |
| `v0.2-week2` | Week 2: Airflow 3.x ETL stack with ETL and Weather DAGs |
| `v0.1-week1` | Week 1: Docker foundations — Flask app containerised |

---

## About

**Ayush Pareek** — Infrastructure & ETL Operations Engineer, LTM, Bengaluru.
Managing Ab Initio ETL infrastructure in production, building toward DevOps/SRE.

- Email: ayushpareek2017@gmail.com
- LinkedIn: linkedin.com/in/ayushpareek
- MCA in ML & AI — Lovely Professional University (2024-2026)
