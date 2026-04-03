# 🚀 Payout Engine

## 📖 The Problem
Processing and loading daily financial accruals with high accuracy and low latency. Calculating massive volumes of payout values with clients' end-of-day (D-1) balances.

## 💡 The Solution
This project implements a hybrid architecture. It's divided into two isolated layers:
1.  **Data Engine (Python):** Executes background processes (batch), calculating the payouts and loading this data in a memory database (Redis), for API consumption, and in DuckDB, for analytics purposes.
2.  **API (Golang):** An API focused on fast reads. It uses *Clean Architecture* and Redis optimization to deliver responses in milliseconds. It has two endpoints:
    -  `net_yield/all`: delivers all the payouts with the respective client account.
    -  `net_yield/{account_id}`: delivers a payout according to the client account (account_id).

## 🛠️ Technologies
* **Golang:** API REST creation.
* **Python (uv):** For data calculation and processib in an isolated layer.
* **Redis & PostgreSQL:** Cache database for fast consumption and relational persistence
* **Docker & Docker Compose:** Containerization of infrastructure (Redis and Postgres) and API, this with Dockerfile.

---

## ⚙️ How to run it

### Prerequisites
* Install [Docker](https://www.docker.com/) and Docker Compose.
* Install [uv](https://docs.astral.sh/uv/) (A Python package manager).

### Step 1: Install Python dependencies
To run the Data Engine, you need to install the packages defined in `pyproject.toml`.
Make sure you are in the root directory and execute:
```bash
uv sync
```

### Step 2: Activate the Infrastructure (Docker)
The Go API, Redis and PostgreSQL are configured to run via containers.
```bash
docker compose up -d
```

### Step 3: Run the Data Engine
With the infrastructure ready, you can run the Data Engine to calculate and load the payout values in Redis and DuckDB.
```bash
uv run data_engine/main.py
```

### Teardown: To deactivate Docker and remove the volumes
```bash
docker compose down -v
```

### To Do
- [] Create an audit function in the Go layer.
- [] Build a Terraform file to provision an AWS infrastructure.
   
