import os from 'os';
import { v4 as uuidv4 } from 'uuid';
import dotenv from 'dotenv';
import { RedisQueue } from '../queue/redisQueue.js';
import { pool } from '../config/db.js';
import { redis } from '../config/redis.js';

dotenv.config();

const WORKER_ID = uuidv4();
const QUEUE_NAME = process.env.QUEUE_NAME || 'notifications-queue';
const CONCURRENCY_LIMIT = 20;

let isRunning = true;
let isShuttingDown = false;
let activeJobCount = 0;
let heartbeatTimer = null;

// 1. Worker Lifecycle (Register & Heartbeat in PostgreSQL)
async function registerWorker() {
  const hostname = os.hostname();
  const pid = process.pid;

  const query = `
    INSERT INTO workers (id, hostname, pid, status, last_heartbeat_at)
    VALUES ($1, $2, $3, 'ACTIVE', NOW())
    ON CONFLICT (id) DO UPDATE 
    SET status = 'ACTIVE', last_heartbeat_at = NOW();
  `;
  await pool.query(query, [WORKER_ID, hostname, pid]);
  console.log(`[Worker Lifecycle] Registered worker ${WORKER_ID} in database.`);

  heartbeatTimer = setInterval(async () => {
    try {
      await pool.query(
        "UPDATE workers SET last_heartbeat_at = NOW() WHERE id = $1",
        [WORKER_ID]
      );
    } catch (err) {
      if (!isShuttingDown) {
        console.error('[Heartbeat Error]:', err.message);
      }
    }
  }, 10000);
}

async function unregisterWorker() {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  try {
    await pool.query(
      "UPDATE workers SET status = 'INACTIVE', last_heartbeat_at = NOW() WHERE id = $1",
      [WORKER_ID]
    );
    console.log(`[Worker Lifecycle] Worker ${WORKER_ID} marked INACTIVE.`);
  } catch (err) {
    console.error('[Unregister Error]:', err.message);
  }
}

// 2. Business Logic Handlers
const taskHandlers = {
  SEND_WELCOME_EMAIL: async (payload) => {
    console.log(`[Task: SEND_WELCOME_EMAIL] Sending email to ${payload.email}...`);
    await new Promise((r) => setTimeout(r, 100));
    return { delivered: true };
  },
  GENERATE_INVOICE: async (payload) => {
    console.log(`[Task: GENERATE_INVOICE] Rendering invoice #${payload.invoiceId}...`);
    await new Promise((r) => setTimeout(r, 200));
    return { invoiceUrl: `https://storage.example.com/invoices/${payload.invoiceId}.pdf` };
  },
  SEND_REMINDER_SMS: async (payload) => {
    console.log(`[Task: SEND_REMINDER_SMS] SMS sent to ${payload.phone}...`);
    await new Promise((r) => setTimeout(r, 80));
    return { sent: true };
  },
  FAILING_TASK_TEST: async () => {
    throw new Error('Simulated network failure for retry & DLQ testing');
  },
};

// 3. Process Job & Sync Database State
async function processJob(job) {
  activeJobCount++;
  const startTime = new Date();
  const startMs = Date.now();
  const attemptNumber = (job.retryCount || 0) + 1;

  try {
    const handler = taskHandlers[job.type] || taskHandlers.SEND_WELCOME_EMAIL;
    await handler(job.payload);

    const finishTime = new Date();
    const durationMs = Date.now() - startMs;

    // A. Clean up Redis RAM
    await RedisQueue.completeJob(QUEUE_NAME, WORKER_ID, job.id);

    // B. Sync PostgreSQL
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

    console.log(`[Worker] Job ${job.id} (${job.type}) COMPLETED in ${durationMs}ms`);
  } catch (err) {
    const finishTime = new Date();
    const durationMs = Date.now() - startMs;
    console.error(`[Worker] Job ${job.id} FAILED (Attempt ${attemptNumber}): ${err.message}`);

    const maxRetries = 3;
    const shouldRetry = (job.retryCount || 0) < maxRetries;

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      await client.query(
        `INSERT INTO job_executions (job_id, worker_id, attempt_number, status, error_message, duration_ms, started_at, finished_at)
         VALUES ($1, $2, $3, 'FAILED', $4, $5, $6, $7)`,
        [job.id, WORKER_ID, attemptNumber, err.message, durationMs, startTime, finishTime]
      );

      if (shouldRetry) {
        const backoffDelayMs = 2000 * Math.pow(2, job.retryCount || 0);
        const nextRunAt = new Date(Date.now() + backoffDelayMs);

        await RedisQueue.retryJob(QUEUE_NAME, WORKER_ID, job.id, (job.retryCount || 0) + 1, backoffDelayMs);

        await client.query(
          `UPDATE jobs 
           SET status = 'QUEUED', retry_count = retry_count + 1, run_at = $1, updated_at = NOW() 
           WHERE id = $2`,
          [nextRunAt, job.id]
        );
        console.log(`[Worker] Job ${job.id} rescheduled for retry at ${nextRunAt.toISOString()}`);
      } else {
        await RedisQueue.moveToDLQ(QUEUE_NAME, WORKER_ID, job.id, err.message);

        await client.query(
          `UPDATE jobs 
           SET status = 'DLQ', updated_at = NOW() 
           WHERE id = $1`,
          [job.id]
        );

        await client.query(
          `INSERT INTO dead_letter_queue (job_id, queue_id, failed_reason)
           VALUES ($1, $2, $3)`,
          [job.id, job.queueId, err.stack || err.message]
        );
        console.warn(`[Worker] Job ${job.id} moved to DLQ (exhausted ${attemptNumber} attempts).`);
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

// 4. Polling Loop
async function startWorkerLoop() {
  await registerWorker();
  console.log(`[Hybrid Redis Worker] Active (Worker ID: ${WORKER_ID}, Queue: "${QUEUE_NAME}"). Polling RAM...`);

  while (isRunning) {
    try {
      const job = await RedisQueue.claimJob(QUEUE_NAME, WORKER_ID, CONCURRENCY_LIMIT);
      if (job) {
        // Execute asynchronously without blocking the loop
        processJob(job).catch((err) => console.error('[Unhandled Process Error]:', err));
      } else {
        await new Promise((r) => setTimeout(r, 100));
      }
    } catch (err) {
      if (isRunning) {
        console.error('[Worker Loop Error]:', err.message);
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }
}

// 5. Graceful Teardown Handler
async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  isRunning = false;

  console.log(`\n[Worker Shutdown] Received ${signal}. Stopping task ingestion...`);

  const shutdownTimeout = setTimeout(() => {
    console.error('[Worker Shutdown] Forcefully terminating due to timeout (15s exceeded).');
    process.exit(1);
  }, 15000);

  // Poll until active jobs reach 0
  while (activeJobCount > 0) {
    console.log(`[Worker Shutdown] Waiting for ${activeJobCount} in-flight job(s) to complete...`);
    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  console.log('[Worker Shutdown] All in-flight jobs completed cleanly.');

  await unregisterWorker();

  try {
    await redis.quit();
    await pool.end();
  } catch (err) {
    console.error('[Resource Cleanup Error]:', err.message);
  }

  clearTimeout(shutdownTimeout);
  console.log('[Worker Shutdown] Teardown complete. Exiting cleanly.');
  process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

startWorkerLoop();