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