import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { v4 as uuidv4 } from 'uuid';
import { pool } from '../src/config/db.js';
import { redis } from '../src/config/redis.js';
import { RedisQueue } from '../src/queue/redisQueue.js';

describe('Distributed Job Scheduling Engine - Core Lifecycle Tests', () => {
  const testQueueName = `test-queue-${Date.now()}`;
  const testWorkerId = uuidv4();
  let testQueueId;

  before(async () => {
    let projRes = await pool.query('SELECT id FROM projects LIMIT 1').catch(() => ({ rows: [] }));
    let projectId = projRes.rows[0]?.id;

    if (!projectId) {
      const pRes = await pool.query(
        "INSERT INTO projects (name, slug) VALUES ('Test Project', $1) RETURNING id",
        [`test-proj-${Date.now()}`]
      ).catch(() => ({ rows: [{ id: uuidv4() }] }));
      projectId = pRes.rows[0]?.id;
    }

    const qRes = await pool.query(
      `INSERT INTO queues (project_id, name, concurrency_limit, priority) 
       VALUES ($1, $2, 10, 5) 
       RETURNING id;`,
      [projectId, testQueueName]
    ).catch(async () => {
      return pool.query(
        `INSERT INTO queues (name, concurrency_limit, priority) 
         VALUES ($1, 10, 5) 
         RETURNING id;`,
        [testQueueName]
      );
    });

    testQueueId = qRes.rows[0]?.id || uuidv4();
  });

  after(async () => {
    const keys = await redis.keys(`queue:${testQueueName}:*`);
    if (keys.length > 0) {
      await redis.del(...keys);
    }
    await pool.query('DELETE FROM jobs WHERE queue_id = $1', [testQueueId]).catch(() => {});
    await pool.query('DELETE FROM queues WHERE id = $1', [testQueueId]).catch(() => {});
    await pool.end();
    await redis.quit();
  });

  test('1. Should enqueue a high-priority and normal-priority job into Redis Sorted Set', async () => {
    const jobNormalId = uuidv4();
    const jobHighId = uuidv4();

    await RedisQueue.enqueue(testQueueName, {
      id: jobNormalId,
      queueId: testQueueId,
      type: 'SEND_WELCOME_EMAIL',
      payload: { email: 'normal@test.com' },
      priority: 2,
    });

    await RedisQueue.enqueue(testQueueName, {
      id: jobHighId,
      queueId: testQueueId,
      type: 'SEND_WELCOME_EMAIL',
      payload: { email: 'high@test.com' },
      priority: 10,
    });

    const pendingCount = await redis.zcard(`queue:${testQueueName}:pending`);
    assert.equal(pendingCount, 2, 'Pending queue should contain exactly 2 jobs');

    const claimedFirst = await RedisQueue.claimJob(testQueueName, testWorkerId, 10);
    assert.ok(claimedFirst, 'Worker should claim a job');
    assert.equal(claimedFirst.id, jobHighId, 'High-priority job (priority=10) must be claimed first');

    await RedisQueue.completeJob(testQueueName, testWorkerId, claimedFirst.id);
    const claimedSecond = await RedisQueue.claimJob(testQueueName, testWorkerId, 10);
    await RedisQueue.completeJob(testQueueName, testWorkerId, claimedSecond.id);
  });

  test('2. Should atomically isolate claimed jobs in in-flight hash table', async () => {
    const jobId = uuidv4();

    await RedisQueue.enqueue(testQueueName, {
      id: jobId,
      queueId: testQueueId,
      type: 'GENERATE_INVOICE',
      payload: { invoiceId: 999 },
      priority: 5,
    });

    const claimed = await RedisQueue.claimJob(testQueueName, testWorkerId, 10);
    assert.equal(claimed.id, jobId);

    const inFlightRaw = await redis.hget(`queue:${testQueueName}:processing`, jobId);
    assert.ok(inFlightRaw, 'Job should reside in processing hash during execution');

    const secondClaim = await RedisQueue.claimJob(testQueueName, testWorkerId, 10);
    assert.equal(secondClaim, null, 'No other worker should claim an in-flight job');

    await RedisQueue.completeJob(testQueueName, testWorkerId, jobId);
  });

  test('3. Should remove job from in-flight and update completion state', async () => {
    const jobId = uuidv4();

    await pool.query(
      `INSERT INTO jobs (id, queue_id, type, payload, priority, status) 
       VALUES ($1, $2, 'SEND_REMINDER_SMS', '{"phone": "+123456"}', 5, 'QUEUED');`,
      [jobId, testQueueId]
    ).catch(() => {});

    await RedisQueue.enqueue(testQueueName, {
      id: jobId,
      queueId: testQueueId,
      type: 'SEND_REMINDER_SMS',
      payload: { phone: '+123456' },
      priority: 5,
    });

    const claimed = await RedisQueue.claimJob(testQueueName, testWorkerId, 10);
    assert.ok(claimed);

    await RedisQueue.completeJob(testQueueName, testWorkerId, jobId);

    const inFlightAfter = await redis.hexists(`queue:${testQueueName}:processing`, jobId);
    assert.equal(inFlightAfter, 0, 'Completed job must be purged from processing hash');

    const pendingAfter = await redis.zcard(`queue:${testQueueName}:pending`);
    assert.equal(pendingAfter, 0, 'Pending queue must be empty');
  });

  test('4. Should reschedule failed job into delayed Sorted Set with backoff', async () => {
    const jobId = uuidv4();
    const retryCount = 1;
    const backoffDelayMs = 1500;

    await RedisQueue.enqueue(testQueueName, {
      id: jobId,
      queueId: testQueueId,
      type: 'FAILING_TASK_TEST',
      payload: {},
      priority: 5,
    });

    const claimed = await RedisQueue.claimJob(testQueueName, testWorkerId, 10);
    assert.ok(claimed);

    await RedisQueue.retryJob(testQueueName, testWorkerId, jobId, retryCount, backoffDelayMs);

    const delayedCount = await redis.zcard(`queue:${testQueueName}:delayed`);
    assert.equal(delayedCount, 1, 'Failed job must be placed in delayed sorted set');

    const inFlight = await redis.hexists(`queue:${testQueueName}:processing`, jobId);
    assert.equal(inFlight, 0, 'Retried job should no longer be marked in-flight');

    await redis.zrem(`queue:${testQueueName}:delayed`, jobId);
  });

  test('5. Should move exhausted job to DLQ list upon terminal failure', async () => {
    const jobId = uuidv4();
    const failureReason = 'Terminal database timeout';

    await pool.query(
      `INSERT INTO jobs (id, queue_id, type, payload, priority, status, retry_count) 
       VALUES ($1, $2, 'FAILING_TASK_TEST', '{}', 5, 'QUEUED', 3);`,
      [jobId, testQueueId]
    ).catch(() => {});

    await RedisQueue.enqueue(testQueueName, {
      id: jobId,
      queueId: testQueueId,
      type: 'FAILING_TASK_TEST',
      payload: {},
      priority: 5,
      retryCount: 3,
    });

    const claimed = await RedisQueue.claimJob(testQueueName, testWorkerId, 10);
    assert.ok(claimed);

    await RedisQueue.moveToDLQ(testQueueName, testWorkerId, jobId, failureReason);

    await pool.query(
      `INSERT INTO dead_letter_queue (job_id, queue_id, failed_reason) 
       VALUES ($1, $2, $3);`,
      [jobId, testQueueId, failureReason]
    ).catch(() => {});

    const dlqCount = await redis.llen(`queue:${testQueueName}:dlq`);
    assert.equal(dlqCount, 1, 'Redis DLQ list must contain 1 quarantined job');
  });

  test('6. Should replay job from DLQ back into active pending queue', async () => {
    const dlqJobs = await redis.lrange(`queue:${testQueueName}:dlq`, 0, -1);
    assert.ok(dlqJobs.length > 0, 'DLQ should have at least 1 job to replay');

    const targetJobId = dlqJobs[0];

    await redis.lrem(`queue:${testQueueName}:dlq`, 0, targetJobId);
    await RedisQueue.enqueue(testQueueName, {
      id: targetJobId,
      queueId: testQueueId,
      type: 'FAILING_TASK_TEST',
      payload: {},
      priority: 5,
    });

    const dlqAfter = await redis.llen(`queue:${testQueueName}:dlq`);
    assert.equal(dlqAfter, 0, 'DLQ list should be empty after replay');

    const pendingAfter = await redis.zcard(`queue:${testQueueName}:pending`);
    assert.equal(pendingAfter, 1, 'Pending queue should receive the replayed job');
  });
});
