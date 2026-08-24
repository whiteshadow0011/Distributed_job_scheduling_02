import { pool } from '../../config/db.js';
import cronParser from 'cron-parser';
import { RedisQueue } from '../../queue/redisQueue.js';


// Ingest a Single Job (Immediate, Delayed, or Scheduled)
export async function createJob(req, res, next) {
  try {
    const { queueId, type, payload = {}, priority = 1, runAt = null } = req.body;

    if (!queueId || !type) {
      return res.status(400).json({ error: 'queueId and type are required fields.' });
    }

    // 1. Verify queue exists and get queue name
    const queueResult = await pool.query(
      'SELECT id, name, concurrency_limit FROM queues WHERE id = $1',
      [queueId]
    );

    if (queueResult.rows.length === 0) {
      return res.status(404).json({ error: 'Target queue does not exist.' });
    }

    const queue = queueResult.rows[0];

    // 2. Persist durably in PostgreSQL
    const insertQuery = `
      INSERT INTO jobs (queue_id, type, payload, priority, status, run_at)
      VALUES ($1, $2, $3, $4, 'QUEUED', COALESCE($5, NOW()))
      RETURNING id, queue_id, type, payload, priority, status, run_at, created_at;
    `;

    const result = await pool.query(insertQuery, [
      queueId,
      type,
      JSON.stringify(payload),
      priority,
      runAt,
    ]);

    const createdJob = result.rows[0];

    // 3. Push to Redis RAM Queue for microsecond dispatching
    await RedisQueue.enqueue(queue.name, {
      id: createdJob.id,          // Pass the exact Postgres UUID
      queueId: createdJob.queue_id,
      type: createdJob.type,
      payload: createdJob.payload,
      priority: createdJob.priority,
      runAt: createdJob.run_at,
    });

    return res.status(202).json({
      message: 'Job accepted and queued for execution.',
      job: createdJob,
    });
  } catch (error) {
    next(error);
  }
}

// Ingest Batch Jobs
export async function createBatchJobs(req, res, next) {
  const client = await pool.connect();
  try {
    const { queueId, jobs } = req.body;

    if (!queueId || !Array.isArray(jobs) || jobs.length === 0) {
      return res.status(400).json({ error: 'queueId and non-empty jobs array are required.' });
    }

    // 1. Verify queue
    const queueResult = await client.query(
      'SELECT id, name FROM queues WHERE id = $1',
      [queueId]
    );

    if (queueResult.rows.length === 0) {
      return res.status(404).json({ error: 'Queue not found.' });
    }

    const queueName = queueResult.rows[0].name;

    await client.query('BEGIN');

    // 2. Build multi-row parameterized query for PostgreSQL
    const values = [];
    const placeholders = jobs.map((job, idx) => {
      const offset = idx * 5;
      values.push(
        queueId,
        job.type,
        JSON.stringify(job.payload || {}),
        job.priority || 1,
        job.runAt || null
      );
      return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, 'QUEUED', COALESCE($${offset + 5}, NOW()))`;
    });

    const insertQuery = `
      INSERT INTO jobs (queue_id, type, payload, priority, status, run_at)
      VALUES ${placeholders.join(', ')}
      RETURNING id, queue_id, type, payload, priority, status, run_at;
    `;

    const { rows: insertedJobs } = await client.query(insertQuery, values);
    await client.query('COMMIT');

    // 3. Batch push to Redis RAM in parallel
    await Promise.all(
      insertedJobs.map((job) =>
        RedisQueue.enqueue(queueName, {
          id: job.id,
          queueId: job.queue_id,
          type: job.type,
          payload: job.payload,
          priority: job.priority,
          runAt: job.run_at,
        })
      )
    );

    return res.status(202).json({
      message: `Successfully accepted batch of ${insertedJobs.length} jobs.`,
      count: insertedJobs.length,
      jobs: insertedJobs,
    });
  } catch (error) {
    await client.query('ROLLBACK');
    next(error);
  } finally {
    client.release();
  }
}

// Ingest Recurring Scheduled (Cron) Job
export async function createRecurringJob(req, res, next) {
  const { queueId, name, cronExpression, jobType, payload = {} } = req.body;

  if (!queueId || !name || !cronExpression || !jobType) {
    return res.status(400).json({ error: 'queueId, name, cronExpression, and jobType are required' });
  }

  try {
    const interval = cronParser.parseExpression(cronExpression);
    const nextRunAt = interval.next().toDate();

    const result = await pool.query(
      `INSERT INTO scheduled_jobs (queue_id, name, cron_expression, job_type, payload, next_run_at)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING *`,
      [queueId, name, cronExpression, jobType, payload, nextRunAt]
    );

    res.status(201).json(result.rows[0]);
  } catch (error) {
    next(error);
  }
}

// List Jobs with Filtering and Pagination
export async function getJobs(req, res, next) {
  const { queueId, status, limit = 20, offset = 0 } = req.query;

  try {
    let query = `SELECT * FROM jobs WHERE 1=1`;
    const params = [];

    if (queueId) {
      params.push(queueId);
      query += ` AND queue_id = $${params.length}`;
    }

    if (status) {
      params.push(status);
      query += ` AND status = $${params.length}`;
    }

    params.push(limit, offset);
    query += ` ORDER BY created_at DESC LIMIT $${params.length - 1} OFFSET $${params.length}`;

    const result = await pool.query(query, params);
    res.json(result.rows);
  } catch (error) {
    next(error);
  }
}
