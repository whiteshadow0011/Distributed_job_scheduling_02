import os from 'os';
import { v4 as uuidv4 } from 'uuid';
import dotenv from 'dotenv';
import { pool } from '../config/db.js';
import { claimNextJob } from './poller.js';
import { executeJob } from './executor.js';

dotenv.config();

const WORKER_ID = uuidv4();
const HOSTNAME = os.hostname();
const PID = process.pid;
const POLL_INTERVAL_MS = 1000;
const HEARTBEAT_INTERVAL_MS = 5000;

let isRunning = true;

// 1. Worker Registration in DB
async function registerWorker() {
  await pool.query(
    `INSERT INTO workers (id, hostname, pid, status, last_heartbeat_at)
     VALUES ($1, $2, $3, 'ACTIVE', NOW())`,
    [WORKER_ID, HOSTNAME, PID]
  );
  console.log(`[Worker Boot] Registered worker ${WORKER_ID} (PID: ${PID}, Host: ${HOSTNAME})`);
}

// 2. Periodic Heartbeat
function startHeartbeat() {
  setInterval(async () => {
    if (!isRunning) return;
    try {
      await pool.query(
        `UPDATE workers SET last_heartbeat_at = NOW() WHERE id = $1`,
        [WORKER_ID]
      );
    } catch (err) {
      console.error('[Worker Heartbeat Error]:', err.message);
    }
  }, HEARTBEAT_INTERVAL_MS);
}

// 3. Continuous Polling Loop
async function startWorkerLoop() {
  console.log('[Worker Engine] Polling loop started. Watching for jobs...');

  while (isRunning) {
    try {
      const job = await claimNextJob(WORKER_ID);

      if (job) {
        // Execute claimed job
        await executeJob(job, WORKER_ID);
      } else {
        // No jobs available or queue at concurrency limit; wait before next poll
        await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
      }
    } catch (error) {
      console.error('[Worker Loop Error]:', error.message);
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
}

// 4. Graceful Shutdown
async function shutdown() {
  console.log('\n[Worker Shutdown] Gracefully stopping worker...');
  isRunning = false;

  try {
    await pool.query(
      `UPDATE workers SET status = 'INACTIVE' WHERE id = $1`,
      [WORKER_ID]
    );
    console.log('[Worker Shutdown] Worker marked INACTIVE.');
  } catch (err) {
    console.error('[Worker Shutdown Error]:', err.message);
  } finally {
    process.exit(0);
  }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// Launch
async function main() {
  await registerWorker();
  startHeartbeat();
  await startWorkerLoop();
}

main().catch((err) => {
  console.error('[Worker Fatal Error]:', err);
  process.exit(1);
});