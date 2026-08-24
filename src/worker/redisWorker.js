import os from 'os';
import { v4 as uuidv4 } from 'uuid';
import dotenv from 'dotenv';
import { RedisQueue } from '../queue/redisQueue.js';
import { pool } from '../config/db.js';
import { redis } from '../config/redis.js';

dotenv.config();

const WORKER_ID = uuidv4();
const CONCURRENCY_LIMIT = 20;

let isRunning = true;
let isShuttingDown = false;
let activeJobCount = 0;
let heartbeatTimer = null;

// Dynamic active queue cache
let cachedQueues = [];
let lastQueueFetch = 0;

async function getActiveQueues() {
  const now = Date.now();
  if (now - lastQueueFetch < 5000 && cachedQueues.length > 0) {
    return cachedQueues;
  }
  try {
    const res = await pool.query(`SELECT name FROM queues WHERE is_paused = false`);
    cachedQueues = res.rows.map(r => r.name);
    lastQueueFetch = now;
  } catch (err) {
    console.error('[Queue Fetch Error]:', err.message);
  }
  return cachedQueues;
}

// 1. Worker Lifecycle
async function registerWorker() {
  const hostname = os.hostname();
  const pid = process.pid;

  const sendHeartbeat = async () => {
    try {
      await pool.query(
        `INSERT INTO workers (id, worker_id, hostname, pid, status, last_heartbeat_at)
         VALUES ($1, $1, $2, $3, 'ACTIVE', NOW())
         ON CONFLICT (id) DO UPDATE 
         SET status = 'ACTIVE', last_heartbeat_at = NOW();`,
        [WORKER_ID, hostname, pid]
      );

      const payload = JSON.stringify({
        id: WORKER_ID,
        hostname,
        pid,
        status: 'ACTIVE',
        concurrency: CONCURRENCY_LIMIT,
        lastHeartbeat: new Date().toISOString(),
      });
      await redis.set(`worker:heartbeat:${WORKER_ID}`, payload, 'EX', 15);
    } catch (err) {
      if (!isShuttingDown) {
        console.error('[Heartbeat Error]:', err.message);
      }
    }
  };

  await sendHeartbeat();
  console.log(`[Worker Lifecycle] Registered worker ${WORKER_ID} in database.`);
  heartbeatTimer = setInterval(sendHeartbeat, 5000);
}

async function unregisterWorker() {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  try {
    await pool.query(
      "UPDATE workers SET status = 'INACTIVE', last_heartbeat_at = NOW() WHERE id = $1",
      [WORKER_ID]
    ).catch(() => {});
    await redis.del(`worker:heartbeat:${WORKER_ID}`).catch(() => {});
    console.log(`[Worker Lifecycle] Worker ${WORKER_ID} marked INACTIVE.`);
  } catch (err) {
    console.error('[Unregister Error]:', err.message);
  }
}

// 2. Business Logic Handlers
const taskHandlers = {
  SEND_WELCOME_EMAIL: async () => {
    await new Promise((r) => setTimeout(r, 60));
    return { delivered: true };
  },
  GENERATE_INVOICE: async (payload) => {
    await new Promise((r) => setTimeout(r, 100));
    return { invoiceUrl: `https://storage.example.com/invoices/${payload?.invoiceId || 'INV-101'}.pdf` };
  },
  SEND_REMINDER_SMS: async () => {
    await new Promise((r) => setTimeout(r, 50));
    return { sent: true };
  },
  FAILING_TASK_TEST: async () => {
    throw new Error('Simulated network failure for retry & DLQ testing');
  },
};

// 3. Process Job
async function processJob(job, queueName) {
  activeJobCount++;
  const startTime = new Date();
  const startMs = Date.now();
  const attemptNumber = (job.retryCount || 0) + 1;

  try {
    const handler = taskHandlers[job.type] || taskHandlers.SEND_WELCOME_EMAIL;
    await handler(job.payload);

    const finishTime = new Date();
    const durationMs = Date.now() - startMs;

    await RedisQueue.completeJob(queueName, WORKER_ID, job.id);

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `UPDATE jobs 
         SET status = 'COMPLETED', locked_by_worker_id = NULL, locked_at = NULL, updated_at = NOW() 
         WHERE id = $1`,
        [job.id]
      );
      await client.query(
        `INSERT INTO job_executions (job_id, worker_id, attempt_number, status, duration_ms, started_at, finished_at)
         VALUES ($1, $2, $3, 'SUCCESS', $4, $5, $6)`,
        [job.id, WORKER_ID, attemptNumber, durationMs, startTime, finishTime]
      );
      await client.query('COMMIT');
    } catch (dbErr) {
      await client.query('ROLLBACK');
      console.error('[DB Sync Error]:', dbErr.message);
    } finally {
      client.release();
    }

    console.log(`[Worker] Job ${job.id?.slice(0, 8) || job.id} (${job.type}) COMPLETED in ${durationMs}ms [Queue: ${queueName}]`);
  } catch (err) {
    const finishTime = new Date();
    const durationMs = Date.now() - startMs;
    console.error(`[Worker] Job ${job.id?.slice(0, 8) || job.id} FAILED: ${err.message}`);

    const maxRetries = 3;
    const shouldRetry = (job.retryCount || 0) < maxRetries - 1;

    let actualQueueId = job.queueId;
    if (!actualQueueId) {
      const qRes = await pool.query('SELECT queue_id FROM jobs WHERE id = $1', [job.id]).catch(() => ({ rows: [] }));
      actualQueueId = qRes.rows[0]?.queue_id;
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO job_executions (job_id, worker_id, attempt_number, status, error_message, duration_ms, started_at, finished_at)
         VALUES ($1, $2, $3, 'FAILED', $4, $5, $6, $7)`,
        [job.id, WORKER_ID, attemptNumber, err.message, durationMs, startTime, finishTime]
      );

      if (shouldRetry) {
        const nextRetryCount = (job.retryCount || 0) + 1;
        const backoffDelayMs = 1500 * Math.pow(2, job.retryCount || 0);
        const nextRunAt = new Date(Date.now() + backoffDelayMs);

        await RedisQueue.retryJob(queueName, WORKER_ID, job.id, nextRetryCount, backoffDelayMs);
        await client.query(
          `UPDATE jobs 
           SET status = 'QUEUED', retry_count = $1, run_at = $2, updated_at = NOW() 
           WHERE id = $3`,
          [nextRetryCount, nextRunAt, job.id]
        );
      } else {
        await RedisQueue.moveToDLQ(queueName, WORKER_ID, job.id, err.message);
        await client.query(`UPDATE jobs SET status = 'DLQ', updated_at = NOW() WHERE id = $1`, [job.id]);
        if (actualQueueId) {
          await client.query(
            `INSERT INTO dead_letter_queue (job_id, queue_id, failed_reason) VALUES ($1, $2, $3)`,
            [job.id, actualQueueId, err.message]
          );
        }
      }
      await client.query('COMMIT');
    } catch (dbErr) {
      await client.query('ROLLBACK');
      console.error('[DB Sync Error during Failure]:', dbErr.message);
    } finally {
      client.release();
    }
  } finally {
    activeJobCount--;
  }
}

// 4. Polling Loop Across All Active Queues
async function startWorkerLoop() {
  await registerWorker();
  console.log(`[Hybrid Redis Worker] Multi-Queue Dispatcher Active (Worker ID: ${WORKER_ID}). Polling all queues...`);

  while (isRunning) {
    let claimedAny = false;

    try {
      const activeQueues = await getActiveQueues();

      for (const queueName of activeQueues) {
        if (activeJobCount >= CONCURRENCY_LIMIT) break;

        // Check delayed jobs for this queue
        const now = Date.now();
        const delayedKey = `queue:${queueName}:delayed`;
        const dueJobs = await redis.zrangebyscore(delayedKey, 0, now);
        if (dueJobs && dueJobs.length > 0) {
          const pipe = redis.pipeline();
          for (const item of dueJobs) {
            pipe.zrem(delayedKey, item);
            let parsed;
            try { parsed = JSON.parse(item); } catch { parsed = { id: item }; }
            pipe.zadd(`queue:${queueName}:pending`, parsed.priority || 5, item);
          }
          await pipe.exec();
        }

        // Claim pending jobs for this queue
        const job = await RedisQueue.claimJob(queueName, WORKER_ID, CONCURRENCY_LIMIT);
        if (job) {
          claimedAny = true;
          processJob(job, queueName).catch((err) => console.error('[Unhandled Process Error]:', err));
        }
      }
    } catch (err) {
      if (isRunning) console.error('[Worker Loop Error]:', err.message);
    }

    if (!claimedAny) {
      await new Promise((r) => setTimeout(r, 100));
    }
  }
}

// 5. Graceful Teardown
async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  isRunning = false;
  console.log(`\n[Worker Shutdown] Received ${signal}. Draining tasks...`);

  while (activeJobCount > 0) {
    await new Promise((resolve) => setTimeout(resolve, 300));
  }

  await unregisterWorker();
  try {
    await redis.quit();
    await pool.end();
  } catch (err) {}
  process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

startWorkerLoop();
