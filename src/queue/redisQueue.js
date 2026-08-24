import { redis } from '../config/redis.js';

export class RedisQueue {
  /**
   * Enqueue job to Redis Priority Sorted Set (ZSET)
   * Score = Priority (higher number = higher priority)
   */
  static async enqueue(queueName, jobData) {
    const serializedJob = JSON.stringify(jobData);
    const score = jobData.priority || 1;

    // Check if delayed
    if (jobData.runAt && new Date(jobData.runAt) > new Date()) {
      const delayTimestamp = new Date(jobData.runAt).getTime();
      await redis.zadd(`queue:${queueName}:delayed`, delayTimestamp, serializedJob);
      return;
    }

    // Pending queue as Sorted Set by priority score
    await redis.zadd(`queue:${queueName}:pending`, score, serializedJob);
  }

  /**
   * Atomically claim the highest-priority job from ZSET
   */
  static async claimJob(queueName, workerId, concurrencyLimit = 20) {
    // 1. Check concurrency
    const activeCount = await redis.get(`queue:${queueName}:active_count`);
    if (parseInt(activeCount || '0', 10) >= concurrencyLimit) {
      return null;
    }

    // 2. Atomically pop highest priority job (ZPOPMAX pops highest score)
    const result = await redis.zpopmax(`queue:${queueName}:pending`, 1);
    if (!result || result.length === 0) {
      return null;
    }

    const [serializedJob, priorityScore] = result;
    const job = JSON.parse(serializedJob);

    // 3. Mark in-flight
    await redis.incr(`queue:${queueName}:active_count`);
    await redis.hset(`queue:${queueName}:processing`, job.id, JSON.stringify({ ...job, claimedBy: workerId }));

    return job;
  }

  /**
   * Mark job completed
   */
  static async completeJob(queueName, workerId, jobId) {
    const pipeline = redis.pipeline();
    pipeline.hdel(`queue:${queueName}:processing`, jobId);
    pipeline.decr(`queue:${queueName}:active_count`);
    await pipeline.exec();
  }

  /**
   * Retry job with backoff (inserts into delayed ZSET)
   */
  static async retryJob(queueName, workerId, jobId, retryCount, delayMs) {
    const executionData = await redis.hget(`queue:${queueName}:processing`, jobId);
    if (!executionData) return;

    const job = JSON.parse(executionData);
    job.retryCount = retryCount;

    const executeAt = Date.now() + delayMs;

    const pipeline = redis.pipeline();
    pipeline.hdel(`queue:${queueName}:processing`, jobId);
    pipeline.decr(`queue:${queueName}:active_count`);
    pipeline.zadd(`queue:${queueName}:delayed`, executeAt, JSON.stringify(job));
    await pipeline.exec();
  }

  /**
   * Move to DLQ
   */
  static async moveToDLQ(queueName, workerId, jobId, errorReason) {
    const executionData = await redis.hget(`queue:${queueName}:processing`, jobId);
    const pipeline = redis.pipeline();
    pipeline.hdel(`queue:${queueName}:processing`, jobId);
    pipeline.decr(`queue:${queueName}:active_count`);
    pipeline.rpush(`queue:${queueName}:dlq`, jobId);
    await pipeline.exec();
  }
}