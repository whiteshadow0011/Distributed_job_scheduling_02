import { pool } from '../../config/db.js';
import { redis } from '../../config/redis.js';

/**
 * GET /api/v1/metrics/overview
 * Global analytics across all queues for the user's organization.
 */
export async function getGlobalOverviewMetrics(req, res, next) {
  try {
    const organizationId = req.user?.organization_id;

    // 1. Fetch all queues belonging to this organization's projects
    const queuesQuery = `
      SELECT q.id, q.name, q.concurrency_limit, q.is_paused, q.priority
      FROM queues q
      JOIN projects p ON q.project_id = p.id
      WHERE p.organization_id = $1
      ORDER BY q.created_at ASC;
    `;
    const { rows: queues } = await pool.query(queuesQuery, [organizationId]);

    if (queues.length === 0) {
      return res.status(200).json({
        totalQueues: 0,
        activeQueues: 0,
        pausedQueues: 0,
        live: {
          totalPendingJobs: 0,
          totalDelayedJobs: 0,
          totalDeadLetterJobs: 0,
          totalActiveWorkerNodes: 0,
        },
        performance24h: {
          totalProcessed: 0,
          completedCount: 0,
          failedCount: 0,
          successRatePercentage: 100.0,
          avgDurationMs: 0,
        },
        queuesSummary: [],
      });
    }

    const queueIds = queues.map((q) => q.id);

    // 2. Fetch Live Redis RAM counts across all queues using a single pipeline
    const redisPipeline = redis.pipeline();
    for (const q of queues) {
      redisPipeline.zcard(`queue:${q.name}:pending`);
      redisPipeline.zcard(`queue:${q.name}:delayed`);
      redisPipeline.llen(`queue:${q.name}:dlq`);
    }
    const pipelineResults = await redisPipeline.exec();

    let totalPending = 0;
    let totalDelayed = 0;
    let totalDlq = 0;

    const queuesSummary = queues.map((q, idx) => {
      const pending = pipelineResults[idx * 3]?.[1] || 0;
      const delayed = pipelineResults[idx * 3 + 1]?.[1] || 0;
      const dlq = pipelineResults[idx * 3 + 2]?.[1] || 0;

      totalPending += pending;
      totalDelayed += delayed;
      totalDlq += dlq;

      return {
        id: q.id,
        name: q.name,
        isPaused: q.is_paused,
        concurrencyLimit: q.concurrency_limit,
        pendingJobs: pending,
        delayedJobs: delayed,
        deadLetterJobs: dlq,
      };
    });

    // 3. PostgreSQL Aggregate Metrics across all organization queues
    const statsQuery = `
      SELECT 
        COUNT(*) FILTER (WHERE je.status = 'SUCCESS') AS completed_24h,
        COUNT(*) FILTER (WHERE je.status = 'FAILED') AS failed_24h,
        COALESCE(ROUND(AVG(je.duration_ms) FILTER (WHERE je.status = 'SUCCESS')), 0) AS avg_duration_ms
      FROM job_executions je
      JOIN jobs j ON je.job_id = j.id
      WHERE j.queue_id = ANY($1::uuid[])
        AND je.started_at >= NOW() - INTERVAL '24 hours';
    `;

    const activeWorkersQuery = `
      SELECT COUNT(*) AS active_workers_count
      FROM workers
      WHERE status = 'ACTIVE' 
        AND last_heartbeat_at >= NOW() - INTERVAL '30 seconds';
    `;

    const [statsResult, activeWorkersResult] = await Promise.all([
      pool.query(statsQuery, [queueIds]),
      pool.query(activeWorkersQuery),
    ]);

    const stats = statsResult.rows[0];
    const completed24h = parseInt(stats.completed_24h || '0', 10);
    const failed24h = parseInt(stats.failed_24h || '0', 10);
    const totalProcessed24h = completed24h + failed24h;
    const successRate = totalProcessed24h > 0 ? ((completed24h / totalProcessed24h) * 100).toFixed(2) : '100.00';

    return res.status(200).json({
      totalQueues: queues.length,
      activeQueues: queues.filter((q) => !q.is_paused).length,
      pausedQueues: queues.filter((q) => q.is_paused).length,
      live: {
        totalPendingJobs: totalPending,
        totalDelayedJobs: totalDelayed,
        totalDeadLetterJobs: totalDlq,
        totalActiveWorkerNodes: parseInt(activeWorkersResult.rows[0]?.active_workers_count || '0', 10),
      },
      performance24h: {
        totalProcessed: totalProcessed24h,
        completedCount: completed24h,
        failedCount: failed24h,
        successRatePercentage: parseFloat(successRate),
        avgDurationMs: parseInt(stats.avg_duration_ms, 10),
      },
      queuesSummary,
    });
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/v1/queues/:queueId/metrics
 * Single Queue metrics
 */
export async function getQueueMetrics(req, res, next) {
  try {
    const { queueId } = req.params;

    const queueResult = await pool.query(
      `SELECT id, name, concurrency_limit, is_paused, priority, created_at 
       FROM queues 
       WHERE id = $1`,
      [queueId]
    );

    if (queueResult.rows.length === 0) {
      return res.status(404).json({ error: 'Queue not found.' });
    }

    const queue = queueResult.rows[0];

    const [pendingCount, delayedCount, dlqCount, activeWorkersRunning] = await Promise.all([
      redis.zcard(`queue:${queue.name}:pending`),
      redis.zcard(`queue:${queue.name}:delayed`),
      redis.llen(`queue:${queue.name}:dlq`),
      redis.get(`queue:${queue.name}:active_count`),
    ]);

    const statsQuery = `
      SELECT 
        COUNT(*) FILTER (WHERE je.status = 'SUCCESS') AS completed_24h,
        COUNT(*) FILTER (WHERE je.status = 'FAILED') AS failed_24h,
        COALESCE(ROUND(AVG(je.duration_ms) FILTER (WHERE je.status = 'SUCCESS')), 0) AS avg_duration_ms,
        COALESCE(MAX(je.duration_ms) FILTER (WHERE je.status = 'SUCCESS'), 0) AS max_duration_ms
      FROM job_executions je
      JOIN jobs j ON je.job_id = j.id
      WHERE j.queue_id = $1 
        AND je.started_at >= NOW() - INTERVAL '24 hours';
    `;

    const activeWorkersQuery = `
      SELECT COUNT(*) AS active_workers_count
      FROM workers
      WHERE status = 'ACTIVE' 
        AND last_heartbeat_at >= NOW() - INTERVAL '30 seconds';
    `;

    const timeSeriesQuery = `
      WITH time_buckets AS (
        SELECT generate_series(
          date_trunc('hour', NOW() - INTERVAL '23 hours'),
          date_trunc('hour', NOW()),
          INTERVAL '1 hour'
        ) AS bucket
      )
      SELECT 
        TO_CHAR(tb.bucket, 'YYYY-MM-DD"T"HH24:00:00"Z"') AS time_bucket,
        COUNT(je.id) FILTER (WHERE je.status = 'SUCCESS') AS completed,
        COUNT(je.id) FILTER (WHERE je.status = 'FAILED') AS failed,
        COALESCE(ROUND(AVG(je.duration_ms) FILTER (WHERE je.status = 'SUCCESS')), 0) AS avg_latency_ms
      FROM time_buckets tb
      LEFT JOIN jobs j ON j.queue_id = $1
      LEFT JOIN job_executions je ON je.job_id = j.id 
        AND date_trunc('hour', je.started_at) = tb.bucket
      GROUP BY tb.bucket
      ORDER BY tb.bucket ASC;
    `;

    const [statsResult, activeWorkersResult, timeSeriesResult] = await Promise.all([
      pool.query(statsQuery, [queueId]),
      pool.query(activeWorkersQuery),
      pool.query(timeSeriesQuery, [queueId]),
    ]);

    const stats = statsResult.rows[0];
    const completed24h = parseInt(stats.completed_24h || '0', 10);
    const failed24h = parseInt(stats.failed_24h || '0', 10);
    const totalProcessed24h = completed24h + failed24h;
    const successRate = totalProcessed24h > 0 ? ((completed24h / totalProcessed24h) * 100).toFixed(2) : '100.00';

    return res.status(200).json({
      queue: {
        id: queue.id,
        name: queue.name,
        isPaused: queue.is_paused,
        concurrencyLimit: queue.concurrency_limit,
      },
      live: {
        pendingJobs: pendingCount,
        delayedJobs: delayedCount,
        deadLetterJobs: dlqCount,
        activeInFlightJobs: parseInt(activeWorkersRunning || '0', 10),
        activeWorkerNodes: parseInt(activeWorkersResult.rows[0]?.active_workers_count || '0', 10),
      },
      performance24h: {
        totalProcessed: totalProcessed24h,
        completedCount: completed24h,
        failedCount: failed24h,
        successRatePercentage: parseFloat(successRate),
        avgDurationMs: parseInt(stats.avg_duration_ms, 10),
        maxDurationMs: parseInt(stats.max_duration_ms, 10),
      },
      timeSeries24h: timeSeriesResult.rows,
    });
  } catch (error) {
    next(error);
  }
}

/**
 * GET /api/v1/queues/:queueId/stream
 * Server-Sent Events (SSE) real-time streaming endpoint for instant frontend metrics
 */
export async function streamQueueMetrics(req, res) {
  const { queueId } = req.params;

  // 1. Set SSE response headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  // 2. Fetch queue name
  const queueResult = await pool.query('SELECT name FROM queues WHERE id = $1', [queueId]);
  if (queueResult.rows.length === 0) {
    res.write(`data: ${JSON.stringify({ error: 'Queue not found' })}\n\n`);
    return res.end();
  }
  const queueName = queueResult.rows[0].name;

  // 3. Periodic metric push every 1000ms
  const sendMetrics = async () => {
    try {
      const [pendingCount, delayedCount, dlqCount, inFlight] = await Promise.all([
        redis.zcard(`queue:${queueName}:pending`),
        redis.zcard(`queue:${queueName}:delayed`),
        redis.llen(`queue:${queueName}:dlq`),
        redis.get(`queue:${queueName}:active_count`),
      ]);

      const payload = {
        timestamp: new Date().toISOString(),
        pending: pendingCount,
        delayed: delayedCount,
        dlq: dlqCount,
        inFlight: parseInt(inFlight || '0', 10),
      };

      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    } catch (err) {
      console.error('[SSE Stream Error]:', err.message);
    }
  };

  const intervalId = setInterval(sendMetrics, 1000);
  sendMetrics();

  // 4. Clean up connection on client disconnect
  req.on('close', () => {
    clearInterval(intervalId);
    res.end();
  });
}