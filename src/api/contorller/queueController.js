import { pool } from '../../config/db.js';

// List all projects for the organization
export async function getProjects(req, res, next) {
  try {
    const result = await pool.query(
      `SELECT * FROM projects WHERE organization_id = $1 ORDER BY created_at DESC`,
      [req.user.organization_id]
    );
    res.json(result.rows);
  } catch (error) {
    next(error);
  }
}

// Create a queue within a project
export async function createQueue(req, res, next) {
  const { projectId, name, priority = 1, concurrencyLimit = 10, retryPolicy } = req.body;

  if (!projectId || !name) {
    return res.status(400).json({ error: 'projectId and name are required' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Verify project belongs to organization
    const projectCheck = await client.query(
      `SELECT id FROM projects WHERE id = $1 AND organization_id = $2`,
      [projectId, req.user.organization_id]
    );

    if (projectCheck.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Project not found' });
    }

    let retryPolicyId = null;
    if (retryPolicy) {
      const rpRes = await client.query(
        `INSERT INTO retry_policies (project_id, name, strategy, max_retries, base_delay_seconds)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id`,
        [
          projectId,
          retryPolicy.name || `${name}-retry-policy`,
          retryPolicy.strategy || 'EXPONENTIAL',
          retryPolicy.maxRetries || 3,
          retryPolicy.baseDelaySeconds || 5,
        ]
      );
      retryPolicyId = rpRes.rows[0].id;
    }

    const queueRes = await client.query(
      `INSERT INTO queues (project_id, retry_policy_id, name, priority, concurrency_limit)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [projectId, retryPolicyId, name, priority, concurrencyLimit]
    );

    await client.query('COMMIT');
    res.status(201).json(queueRes.rows[0]);
  } catch (error) {
    await client.query('ROLLBACK');
    if (error.code === '23505') {
      return res.status(409).json({ error: 'Queue with this name already exists in project' });
    }
    next(error);
  } finally {
    client.release();
  }
}

// Pause or Resume a Queue
export async function togglePauseQueue(req, res, next) {
  const { queueId } = req.params;
  const { isPaused } = req.body; // boolean

  if (typeof isPaused !== 'boolean') {
    return res.status(400).json({ error: 'isPaused boolean is required' });
  }

  try {
    const result = await pool.query(
      `UPDATE queues 
       SET is_paused = $1 
       WHERE id = $2 
       RETURNING *`,
      [isPaused, queueId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Queue not found' });
    }

    res.json({ message: `Queue ${isPaused ? 'paused' : 'resumed'}`, queue: result.rows[0] });
  } catch (error) {
    next(error);
  }
}

// List all queues with statistics
export async function getQueues(req, res, next) {
  try {
    const query = `
      SELECT 
        q.*,
        COUNT(CASE WHEN j.status = 'QUEUED' THEN 1 END) as queued_count,
        COUNT(CASE WHEN j.status = 'RUNNING' THEN 1 END) as running_count,
        COUNT(CASE WHEN j.status = 'COMPLETED' THEN 1 END) as completed_count,
        COUNT(CASE WHEN j.status = 'DLQ' THEN 1 END) as dlq_count
      FROM queues q
      INNER JOIN projects p ON q.project_id = p.id
      LEFT JOIN jobs j ON q.id = j.queue_id
      WHERE p.organization_id = $1
      GROUP BY q.id
      ORDER BY q.created_at DESC;
    `;
    const result = await pool.query(query, [req.user.organization_id]);
    res.json(result.rows);
  } catch (error) {
    next(error);
  }
}