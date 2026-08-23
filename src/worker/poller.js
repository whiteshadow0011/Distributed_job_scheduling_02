import { pool } from '../config/db.js';

/**
 * Atomically claims the highest-priority eligible job for this worker.
 * @param {string} workerId - UUID of the active worker
 * @returns {Promise<Object|null>} - The locked job row or null if none available
 */
export async function claimNextJob(workerId) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // 1. Find the next eligible job that satisfies concurrency, priority, and pause constraints
    const claimQuery = `
      WITH active_queue_counts AS (
        -- Count how many jobs are currently running per queue
        SELECT queue_id, COUNT(*) as running_count
        FROM jobs
        WHERE status = 'RUNNING'
        GROUP BY queue_id
      ),
      eligible_jobs AS (
        SELECT 
          j.id as job_id,
          j.queue_id,
          j.type,
          j.payload,
          j.retry_count,
          q.concurrency_limit,
          COALESCE(aqc.running_count, 0) as current_running,
          rp.strategy,
          rp.max_retries,
          rp.base_delay_seconds
        FROM jobs j
        INNER JOIN queues q ON j.queue_id = q.id
        LEFT JOIN retry_policies rp ON q.retry_policy_id = rp.id
        LEFT JOIN active_queue_counts aqc ON q.id = aqc.queue_id
        WHERE 
          j.status = 'QUEUED'
          AND j.run_at <= NOW()
          AND q.is_paused = FALSE
          AND COALESCE(aqc.running_count, 0) < q.concurrency_limit
        ORDER BY 
          q.priority DESC,
          j.priority DESC,
          j.run_at ASC,
          j.created_at ASC,
          j.id ASC
        FOR UPDATE OF j SKIP LOCKED
        LIMIT 1
      )
      UPDATE jobs j
      SET 
        status = 'RUNNING',
        locked_by_worker_id = $1,
        locked_at = NOW(),
        updated_at = NOW()
      FROM eligible_jobs ej
      WHERE j.id = ej.job_id
      RETURNING 
        j.id, 
        j.queue_id, 
        j.type, 
        j.payload, 
        j.retry_count,
        ej.strategy,
        ej.max_retries,
        ej.base_delay_seconds;
    `;

    const result = await client.query(claimQuery, [workerId]);

    if (result.rows.length === 0) {
      await client.query('COMMIT');
      return null;
    }

    const claimedJob = result.rows[0];

    // 2. Create an initial 'RUNNING' record in job_executions audit table
    await client.query(
      `INSERT INTO job_executions (job_id, worker_id, attempt_number, status, started_at)
       VALUES ($1, $2, $3, 'RUNNING', NOW())`,
      [claimedJob.id, workerId, claimedJob.retry_count + 1]
    );

    await client.query('COMMIT');
    return claimedJob;
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('[Poller] Error claiming job:', error);
    throw error;
  } finally {
    client.release();
  }
}