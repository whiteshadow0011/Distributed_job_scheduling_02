# ⚡ Distributed Multi-Tenant Job Scheduling & Execution Engine

A fault-tolerant, horizontally scalable, multi-tenant distributed job scheduling and asynchronous execution platform built with Node.js, Redis Sorted Sets, PostgreSQL, and Docker.

---

## 🏗️ Architecture Overview

The system utilizes a dual-tier persistence and queuing model:
* **Redis RAM Engine (`Sorted Sets` + `Pipelines`):** Microsecond-latency priority queuing, delayed task scheduling, atomic job claiming (`ZPOPMIN`), and real-time state transitions.
* **PostgreSQL Engine (`ACID` + `JSONB`):** Durable state tracking, multi-tenant organization isolation, audit logs, and historical execution metrics.
* **Autonomous Worker Fleet:** Scalable, independent worker daemons with heartbeat registration, rate limiting, and exponential backoff failover to Dead Letter Queues (DLQ).

---

## 🚀 Quick Start (Docker Deployment)

### 1. Clone & Start Stack

```bash
# Clone the repository
git clone [https://github.com/whiteshadow0011/Distributed_job_scheduling_02.git](https://github.com/whiteshadow0011/Distributed_job_scheduling_02.git)
cd Distributed_job_scheduling_02

# Start all containers in detached mode
docker compose up --build -d


### 2. Verify Container Health

```bash
docker compose ps

bash -c '
set -e
echo "=========================================================="
echo "  1. AUTHENTICATING & CREATING TENANT"
echo "=========================================================="
AUTH_RES=$(curl -s -X POST http://localhost:5000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d "{\"organizationName\":\"Evaluation Org\",\"email\":\"evaluator@system.local\",\"password\":\"securePass123\"}")

TOKEN=$(echo "$AUTH_RES" | grep -o "\"token\":\"[^\"]*" | cut -d"\"" -f4)
echo "JWT Token Acquired: ${TOKEN:0:28}..."

echo ""
echo "=========================================================="
echo "  2. RETRIEVING DEFAULT PROJECT"
echo "=========================================================="
PROJ_RES=$(curl -s -X GET http://localhost:5000/api/v1/queues/projects \
  -H "Authorization: Bearer $TOKEN")
PROJECT_ID=$(echo "$PROJ_RES" | grep -o "\"id\":\"[^\"]*" | head -n 1 | cut -d"\"" -f4)
echo "Project ID: $PROJECT_ID"

echo ""
echo "=========================================================="
echo "  3. CREATING DYNAMIC JOB QUEUE"
echo "=========================================================="
QUEUE_NAME="eval-batch-queue"
QUEUE_RES=$(curl -s -X POST http://localhost:5000/api/v1/queues \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"projectId\":\"$PROJECT_ID\",\"name\":\"$QUEUE_NAME\",\"concurrencyLimit\":20}")
QUEUE_ID=$(echo "$QUEUE_RES" | grep -o "\"id\":\"[^\"]*" | head -n 1 | cut -d"\"" -f4)
echo "Queue Created: $QUEUE_NAME ($QUEUE_ID)"

echo ""
echo "=========================================================="
echo "  4. INGESTING HIGH-THROUGHPUT BATCH JOBS"
echo "=========================================================="
curl -s -X POST http://localhost:5000/api/v1/jobs/batch \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"queueId\":\"$QUEUE_ID\",\"jobs\":[
    {\"type\":\"SEND_WELCOME_EMAIL\",\"payload\":{\"userId\":\"usr-101\",\"email\":\"eval1@test.com\"}},
    {\"type\":\"GENERATE_INVOICE\",\"payload\":{\"invoiceId\":\"INV-9901\"}},
    {\"type\":\"SEND_REMINDER_SMS\",\"payload\":{\"phone\":\"+1987654321\"}},
    {\"type\":\"SEND_WELCOME_EMAIL\",\"payload\":{\"userId\":\"usr-102\",\"email\":\"eval2@test.com\"}}
  ]}"
echo -e "\n4 Batch Jobs Accepted and Queued."

echo ""
echo "=========================================================="
echo "  5. TESTING EXPONENTIAL BACKOFF RETRY & DLQ"
echo "=========================================================="
curl -s -X POST http://localhost:5000/api/v1/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"queueId\":\"$QUEUE_ID\",\"type\":\"FAILING_TASK_TEST\",\"payload\":{\"simulateFailure\":true}}"
echo -e "\nFailing Task Enqueued (Triggers retry backoff -> DLQ transition)."

echo ""
echo "=========================================================="
echo "  6. FETCHING LIVE OVERVIEW METRICS"
echo "=========================================================="
sleep 2
curl -s -X GET http://localhost:5000/api/v1/metrics/overview \
  -H "Authorization: Bearer $TOKEN"
echo -e "\n\nEvaluation Complete."
'


## ⚙️ Operational Commands

### Live Log Streaming
```bash
# Stream worker task execution
docker compose logs worker -f

# Stream API logs
docker compose logs api -f

# Scale to 3 active worker instances
docker compose up --scale worker=3 -d

# Execute unit & integration test suite
docker compose exec api npm test

# Stop all containers and purge persistent volumes
docker compose down -v

## Scaling Workers Horizontally

The worker pool is completely stateless and utilizes Redis atomic locks and queue primitives to guarantee mutually exclusive job execution without duplicate processing.

### Run with a Custom Number of Workers
To scale out the worker fleet to $N$ instances (e.g., 3 or 5 nodes):

```bash
# Start the full stack with 3 parallel worker processes
docker compose up -d --scale worker=3


# for these diagrams, plase copy and paste these mermaid codes at https://mermaid.ai/app/dashboard

#System Architecture

## 🏛️ System Architecture

```mermaid
flowchart TB
    subgraph ClientLayer ["Client & Management Layer"]
        UI["🖥️ React Dashboard UI (Port 3000)"]
        CLI["💻 Evaluator Scripts / External Apps"]
    end

    subgraph APILayer ["API & Ingestion Service (Port 5000)"]
        API["⚡ Express / Node.js REST Engine"]
        AuthMid["🛡️ JWT Auth & Tenant Validation"]
        RateLmt["⏱️ Rate Limiter"]
        BatchIngest["📦 Batch Job Ingestion"]
        API --> AuthMid --> RateLmt --> BatchIngest
    end

    subgraph MemoryLayer ["Fast Queuing & In-Memory State"]
        Redis[("⚡ Redis 7 Engine (Port 6379)")]
        ZSetReady["🗂️ Ready Queue (ZSET by priority/time)"]
        ZSetDelay["⏰ Delayed Queue (ZSET run_at)"]
        AtomicClaim["🔒 Atomic ZPOPMIN Claiming"]
        Redis --- ZSetReady
        Redis --- ZSetDelay
        Redis --- AtomicClaim
    end

    subgraph PersistenceLayer ["Durable ACID Store"]
        Postgres[("🐘 PostgreSQL 16 DB (Port 5432)")]
        Tables["Organizations | Queues | Jobs | Executions | DLQ"]
        Postgres --- Tables
    end

    subgraph WorkerLayer ["Autonomous Worker Fleet"]
        W1["⚙️ Worker Node 1"]
        W2["⚙️ Worker Node 2"]
        WN["⚙️ Worker Node N (Scale 1..N)"]
    end

    UI -->|REST / SSE Telemetry| API
    CLI -->|HTTP Requests| API

    BatchIngest -->|Enqueue Tasks| Redis
    BatchIngest -->|Durable Persistence| Postgres

    W1 & W2 & WN -->|Atomic ZPOPMIN Poll| Redis
    W1 & W2 & WN -->|State Transition & Telemetry| Postgres
    W1 & W2 & WN -.->|Max Retries Exceeded -> DLQ| Postgres


    # ER Diagram

    ### 2. Entity-Relationship (ER) Diagram

```markdown
## 🗄️ Database Entity-Relationship (ER) Diagram

```mermaid
erDiagram
    ORGANIZATIONS ||--o{ USERS : "has many"
    ORGANIZATIONS ||--o{ PROJECTS : "owns"
    ORGANIZATIONS ||--o{ RETRY_POLICIES : "defines"
    PROJECTS ||--o{ QUEUES : "contains"
    RETRY_POLICIES ||--o{ QUEUES : "applies to"
    QUEUES ||--o{ JOBS : "holds"
    JOBS ||--o{ JOB_EXECUTIONS : "records"
    QUEUES ||--o{ DEAD_LETTER_QUEUE : "isolates failures"

    ORGANIZATIONS {
        uuid id PK
        string name
        timestamp created_at
    }

    USERS {
        uuid id PK
        uuid organization_id FK
        string name
        string email UK
        string password_hash
        string role
        timestamp created_at
    }

    PROJECTS {
        uuid id PK
        uuid organization_id FK
        string name
        string slug
        timestamp created_at
    }

    RETRY_POLICIES {
        uuid id PK
        uuid organization_id FK
        uuid project_id FK
        string name
        string strategy
        int max_retries
        int base_delay_seconds
        int initial_delay_ms
        float backoff_multiplier
        timestamp created_at
    }

    QUEUES {
        uuid id PK
        uuid project_id FK
        uuid retry_policy_id FK
        string name
        int concurrency_limit
        int priority
        boolean is_paused
        int rate_limit
        int rate_limit_window_ms
        timestamp created_at
    }

    JOBS {
        uuid id PK
        uuid queue_id FK
        string type
        jsonb payload
        int priority
        string status
        int retry_count
        int max_retries
        timestamp run_at
        string locked_by_worker_id
        timestamp locked_at
        timestamp started_at
        timestamp completed_at
        text error_message
        timestamp created_at
        timestamp updated_at
    }

    JOB_EXECUTIONS {
        uuid id PK
        uuid job_id FK
        string worker_id
        int attempt_number
        string status
        int duration_ms
        text error_message
        timestamp started_at
        timestamp finished_at
    }

    DEAD_LETTER_QUEUE {
        uuid id PK
        uuid job_id
        uuid queue_id FK
        text failed_reason
        timestamp created_at
    }

    WORKERS {
        string id PK
        string worker_id
        string hostname
        int pid
        string status
        int current_jobs_count
        timestamp last_heartbeat_at
        timestamp created_at
    }