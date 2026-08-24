import { CronExpressionParser } from 'cron-parser';
import dotenv from 'dotenv';
import { pool } from '../config/db.js';
import { RedisQueue } from '../queue/redisQueue.js';
import { redis } from '../config/redis.js';

dotenv.config();

const EVALUATOR_INTERVAL_MS = 5000; // Check recurring cron rules every 5s
const DELAYED_CHECK_INTERVAL_MS = 1000; // Promote ready delayed jobs to pending every 1s
const REAPER_INTERVAL_MS = 15000; // Check for dead/zombie workers every 15s
const ZOMBIE_TIMEOUT_SECONDS = 120; // Reclaim locks older than 2 minutes

// 1. Recurring Cron Evaluator
async function evaluateRecurringJobs() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const dueJobsQuery = `
      SELECT 
        sj.id, 
        sj.queue_id, 
        q.name AS queue_name, 
        sj.name, 
        sj.cron_expression, 
        sj.payload
      FROM scheduled_jobs sj
      JOIN queues q ON sj.queue_id = q.id
      WHERE sj.is_active = TRUE AND sj.next_run_at <= NOW()
      FOR UPDATE OF sj SKIP LOCKED;
    `;
    const { rows: dueJobs } = await client.query(dueJobsQuery);

    for (const job of dueJobs) {
      const defaultPriority = 1;

      // A. Stamp row in PostgreSQL
      const insertResult = await client.query(
        `INSERT INTO jobs (queue_id, type, payload, priority, status, run_at)
         VALUES ($1, $2, $3, $4, 'QUEUED', NOW())
         RETURNING id, queue_id, type, payload, priority, run_at;`,
        [job.queue_id, job.name, JSON.stringify(job.payload || {}), defaultPriority]
      );

      const createdJob = insertResult.rows[0];

      // B. Compute next execution timestamp
      const interval = CronExpressionParser.parse(job.cron_expression);
      const nextRunAt = interval.next().toDate();

      // C. Update scheduled_jobs rule
      await client.query(
        `UPDATE scheduled_jobs 
         SET next_run_at = $1, last_run_at = NOW(), updated_at = NOW() 
         WHERE id = $2`,
        [nextRunAt, job.id]
      );

      // D. Push into Redis RAM
      await RedisQueue.enqueue(job.queue_name, {
        id: createdJob.id,
        queueId: createdJob.queue_id,
        type: createdJob.type,
        payload: createdJob.payload,
        priority: createdJob.priority,
        runAt: createdJob.run_at,
      });

      console.log(`[Cron Evaluator] Enqueued "${job.name}" (${createdJob.id}) to Redis. Next: ${nextRunAt.toISOString()}`);
    }

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('[Cron Evaluator Error]:', error.message);
  } finally {
    client.release();
  }
}

// 2. Delayed Job Promoter (ZSET: delayed -> pending by priority)
async function evaluateDelayedJobs() {
  try {
    const { rows: activeQueues } = await pool.query('SELECT name FROM queues WHERE is_paused = FALSE');
    const now = Date.now();

    for (const queue of activeQueues) {
      const readyJobs = await redis.zrangebyscore(`queue:${queue.name}:delayed`, 0, now);

      if (readyJobs.length > 0) {
        const pipeline = redis.pipeline();
        for (const jobStr of readyJobs) {
          const job = JSON.parse(jobStr);
          pipeline.zrem(`queue:${queue.name}:delayed`, jobStr);
          pipeline.zadd(`queue:${queue.name}:pending`, job.priority || 1, jobStr);
        }
        await pipeline.exec();
        console.log(`[Delayed Promoter] Promoted ${readyJobs.length} delayed jobs in queue "${queue.name}" to pending.`);
      }
    }
  } catch (error) {
    console.error('[Delayed Promoter Error]:', error.message);
  }
}

// 3. Zombie Job Reaper (Self-Healing Engine)
async function reapZombieJobs() {
  try {
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
      result.rows.forEach((r) => console.warn(`   -> Recovered Job ID: ${r.id} (${r.type})`));
    }
  } catch (error) {
    console.error('[Zombie Reaper Error]:', error.message);
  }
}

// 4. Main Scheduler Runner
function startScheduler() {
  console.log('[Scheduler Service] Cron Evaluator, Delayed Promoter & Zombie Reaper started.');

  setInterval(evaluateRecurringJobs, EVALUATOR_INTERVAL_MS);
  setInterval(evaluateDelayedJobs, DELAYED_CHECK_INTERVAL_MS);
  setInterval(reapZombieJobs, REAPER_INTERVAL_MS);

  // Initial runs on boot
  evaluateRecurringJobs();
  evaluateDelayedJobs();
  reapZombieJobs();
}

startScheduler();