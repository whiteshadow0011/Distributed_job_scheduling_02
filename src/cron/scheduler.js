import { CronExpressionParser } from 'cron-parser';
import dotenv from 'dotenv';
import { pool } from '../config/db.js';

dotenv.config();

const EVALUATOR_INTERVAL_MS = 5000; // Check recurring jobs every 5s
const REAPER_INTERVAL_MS = 15000;    // Check for dead/zombie jobs every 15s
const ZOMBIE_TIMEOUT_SECONDS = 120; // Reclaim locks older than 2 minutes

// 1. Recurring Cron Evaluator
async function evaluateRecurringJobs() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Find due recurring jobs and lock them
    const dueJobsQuery = `
      SELECT sj.id, sj.queue_id, sj.name, sj.cron_expression, sj.payload, sj.priority
      FROM scheduled_jobs sj
      WHERE sj.is_active = TRUE AND sj.next_run_at <= NOW()
      FOR UPDATE SKIP LOCKED;
    `;
    const { rows: dueJobs } = await client.query(dueJobsQuery);

    for (const job of dueJobs) {
      // 1. Stamp out active task in jobs table
      await client.query(
        `INSERT INTO jobs (queue_id, type, payload, priority, status, run_at)
         VALUES ($1, $2, $3, $4, 'QUEUED', NOW())`,
        [job.queue_id, job.name, job.payload, job.priority]
      );

      // 2. Compute next execution time
      const interval = CronExpressionParser.parse(job.cron_expression);
      const nextRunAt = interval.next().toDate();

      // 3. Update scheduled_jobs rule
      await client.query(
        `UPDATE scheduled_jobs 
         SET next_run_at = $1, last_run_at = NOW(), updated_at = NOW() 
         WHERE id = $2`,
        [nextRunAt, job.id]
      );

      console.log(`[Cron Evaluator] Enqueued instance for "${job.name}". Next run at ${nextRunAt.toISOString()}`);
    }

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('[Cron Evaluator Error]:', error.message);
  } finally {
    client.release();
  }
}

// 2. Zombie Job Reaper (Self-Healing Engine)
async function reapZombieJobs() {
  try {
    // Reclaim jobs locked by crashed workers (stale lock or dead worker heartbeat)
    const reclaimQuery = `
      UPDATE jobs
      SET 
        status = 'QUEUED',
        locked_by_worker_id = NULL,
        locked_at = NULL,
        retry_count = retry_count + 1,
        updated_at = NOW()
      WHERE status = 'RUNNING'
        AND (
          locked_at < NOW() - INTERVAL '${ZOMBIE_TIMEOUT_SECONDS} seconds'
          OR locked_by_worker_id IN (
            SELECT id FROM workers 
            WHERE status = 'INACTIVE' 
               OR last_heartbeat_at < NOW() - INTERVAL '30 seconds'
          )
        )
      RETURNING id, type, locked_by_worker_id;
    `;

    const result = await pool.query(reclaimQuery);
    if (result.rows.length > 0) {
      console.warn(`[Zombie Reaper] Recovered ${result.rows.length} stuck jobs from dead/stalled workers:`);
      result.rows.forEach((r) => console.warn(`  -> Recovered Job ID: ${r.id} (${r.type})`));
    }
  } catch (error) {
    console.error('[Zombie Reaper Error]:', error.message);
  }
}

// 3. Main Scheduler Runner
function startScheduler() {
  console.log('[Scheduler Service] Cron Evaluator & Zombie Reaper started.');

  // Run intervals
  setInterval(evaluateRecurringJobs, EVALUATOR_INTERVAL_MS);
  setInterval(reapZombieJobs, REAPER_INTERVAL_MS);

  // Initial runs on boot
  evaluateRecurringJobs();
  reapZombieJobs();
}

startScheduler();