import { pool } from '../../config/db.js';
import cronParser from 'cron-parser';

// Ingest a Single Job (Immediate, Delayed, or Scheduled)
export async function createJob(req, res, next) {
  const { queueId, type, payload = {}, priority = 1, delaySeconds, runAt } = req.body;

  if (!queueId || !type) {
    return res.status(400).json({ error: 'queueId and type are required' });
  }

  try {
    let calculatedRunAt = new Date();
    let initialStatus = 'QUEUED';

    if (delaySeconds) {
      calculatedRunAt = new Date(Date.now() + delaySeconds * 1000);
      initialStatus = 'SCHEDULED';
    } else if (runAt) {
      calculatedRunAt = new Date(runAt);
      if (calculatedRunAt > new Date()) {
        initialStatus = 'SCHEDULED';
      }
    }

    const result = await pool.query(
      `INSERT INTO jobs (queue_id, type, payload, priority, status, run_at)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING *`,
      [queueId, type, payload, priority, initialStatus, calculatedRunAt]
    );

    res.status(201).json(result.rows[0]);
  } catch (error) {
    next(error);
  }
}

// Ingest Batch Jobs
export async function createBatchJobs(req, res, next) {
  const { queueId, jobs } = req.body; // jobs is an array: [{ type, payload, priority }]

  if (!queueId || !Array.isArray(jobs) || jobs.length === 0) {
    return res.status(400).json({ error: 'queueId and a non-empty jobs array are required' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const createdJobs = [];

    for (const job of jobs) {
      const res = await client.query(
        `INSERT INTO jobs (queue_id, type, payload, priority, status)
         VALUES ($1, $2, $3, $4, 'QUEUED')
         RETURNING *`,
        [queueId, job.type, job.payload || {}, job.priority || 1]
      );
      createdJobs.push(res.rows[0]);
    }

    await client.query('COMMIT');
    res.status(201).json({ count: createdJobs.length, jobs: createdJobs });
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
