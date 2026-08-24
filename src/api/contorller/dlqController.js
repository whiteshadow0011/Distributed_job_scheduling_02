import { pool } from '../../config/db.js';
import { RedisQueue } from '../../queue/redisQueue.js';
import { redis } from '../../config/redis.js';

/**
 * GET /api/v1/queues/:queueId/dlq
 * Fetch paginated dead-letter jobs with full error history.
 */
export async function getDLQJobs(req, res, next) {
  try {
    const { queueId } = req.params;
    const page = Math.max(1, parseInt(req.query.page || '1', 10));
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit || '50', 10)));
    const offset = (page - 1) * limit;

    // 1. Verify queue exists
    const queueCheck = await pool.query('SELECT id, name FROM queues WHERE id = $1', [queueId]);
    if (queueCheck.rows.length === 0) {
      return res.status(404).json({ error: 'Queue not found.' });
    }

    // 2. Fetch DLQ entries with fallback-safe ordering on jobs.created_at
    const listQuery = `
      SELECT 
        dlq.id AS dlq_id,
        dlq.job_id,
        dlq.failed_reason AS error_message,
        j.type AS job_type,
        j.payload,
        j.priority,
        j.retry_count,
        j.created_at AS job_created_at
      FROM dead_letter_queue dlq
      JOIN jobs j ON dlq.job_id = j.id
      WHERE dlq.queue_id = $1
      ORDER BY j.created_at DESC
      LIMIT $2 OFFSET $3;
    `;

    const countQuery = `
      SELECT COUNT(*) AS total 
      FROM dead_letter_queue 
      WHERE queue_id = $1;
    `;

    const [itemsResult, countResult] = await Promise.all([
      pool.query(listQuery, [queueId, limit, offset]),
      pool.query(countQuery, [queueId]),
    ]);

    const total = parseInt(countResult.rows[0]?.total || '0', 10);
    const rows = itemsResult.rows || [];

    // Return compatibility structures for various UI consumers
    return res.status(200).json({
      data: rows,
      dlqJobs: rows,
      total,
      pagination: {
        total,
        page,
        limit,
        totalPages: Math.ceil(total / limit) || 1,
      },
    });
  } catch (error) {
    console.error('[DLQ Controller Error]:', error.message);
    next(error);
  }
}

/**
 * POST /api/v1/queues/:queueId/dlq/replay
 * Replays dead jobs by removing them from DLQ, resetting status to QUEUED in PostgreSQL,
 * and re-injecting them into Redis RAM for immediate worker execution.
 */
export async function replayDLQJobs(req, res, next) {
  const client = await pool.connect();
  try {
    const { queueId } = req.params;
    const { jobIds } = req.body || {};

    const queueCheck = await client.query('SELECT id, name FROM queues WHERE id = $1', [queueId]);
    if (queueCheck.rows.length === 0) {
      return res.status(404).json({ error: 'Queue not found.' });
    }
    const queue = queueCheck.rows[0];

    await client.query('BEGIN');

    let selectQuery;
    let queryParams;

    if (Array.isArray(jobIds) && jobIds.length > 0) {
      selectQuery = `
        SELECT dlq.id AS dlq_id, dlq.job_id, j.type, j.payload, j.priority
        FROM dead_letter_queue dlq
        JOIN jobs j ON dlq.job_id = j.id
        WHERE dlq.queue_id = $1 AND dlq.job_id = ANY($2::uuid[])
        FOR UPDATE OF dlq;
      `;
      queryParams = [queueId, jobIds];
    } else {
      selectQuery = `
        SELECT dlq.id AS dlq_id, dlq.job_id, j.type, j.payload, j.priority
        FROM dead_letter_queue dlq
        JOIN jobs j ON dlq.job_id = j.id
        WHERE dlq.queue_id = $1
        LIMIT 500
        FOR UPDATE OF dlq;
      `;
      queryParams = [queueId];
    }

    const { rows: jobsToReplay } = await client.query(selectQuery, queryParams);

    if (jobsToReplay.length === 0) {
      await client.query('ROLLBACK');
      return res.status(200).json({ message: 'No eligible DLQ jobs found to replay.', replayedCount: 0 });
    }

    const targetJobIds = jobsToReplay.map((j) => j.job_id);
    const targetDlqIds = jobsToReplay.map((j) => j.dlq_id);

    // 3. Remove from dead_letter_queue table in Postgres
    await client.query('DELETE FROM dead_letter_queue WHERE id = ANY($1::uuid[])', [targetDlqIds]);

    // 4. Reset jobs status in PostgreSQL
    await client.query(
      `UPDATE jobs 
       SET status = 'QUEUED', retry_count = 0, run_at = NOW(), updated_at = NOW() 
       WHERE id = ANY($1::uuid[])`,
      [targetJobIds]
    );

    await client.query('COMMIT');

    // 5. Clean up Redis DLQ list & Re-enqueue to Redis RAM
    const redisPipeline = redis.pipeline();
    for (const job of jobsToReplay) {
      redisPipeline.lrem(`queue:${queue.name}:dlq`, 0, job.job_id);
    }
    await redisPipeline.exec();

    // 6. Re-enqueue into active Redis Queue
    await Promise.all(
      jobsToReplay.map((job) =>
        RedisQueue.enqueue(queue.name, {
          id: job.job_id,
          queueId,
          type: job.type,
          payload: job.payload,
          priority: job.priority || 5,
          runAt: null,
        })
      )
    );

    return res.status(200).json({
      message: `Successfully re-queued ${jobsToReplay.length} jobs from DLQ for execution.`,
      replayedCount: jobsToReplay.length,
      jobIds: targetJobIds,
    });
  } catch (error) {
    await client.query('ROLLBACK');
    next(error);
  } finally {
    client.release();
  }
}