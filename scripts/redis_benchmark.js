import { RedisQueue } from '../src/queue/redisQueue.js';
import { redis } from '../src/config/redis.js';

async function benchmark(totalJobs = 10000) {
  console.log(`[Benchmark] Starting injection of ${totalJobs} jobs into Redis RAM...`);
  const startTime = Date.now();

  const promises = [];
  for (let i = 1; i <= totalJobs; i++) {
    const jobPromise = RedisQueue.enqueue('notifications', {
      type: i % 10 === 0 ? 'FAILING_TASK_TEST' : 'SEND_WELCOME_EMAIL',
      payload: { userId: i, email: `user_${i}@scale.io` },
      priority: (i % 5) + 1,
    });
    promises.push(jobPromise);
  }

  await Promise.all(promises);
  const totalTimeSec = (Date.now() - startTime) / 1000;
  const throughput = Math.round(totalJobs / totalTimeSec);

  console.log(`[Benchmark Complete]`);
  console.log(`  -> Ingested: ${totalJobs} jobs`);
  console.log(`  -> Time taken: ${totalTimeSec.toFixed(2)}s`);
  console.log(`  -> Ingestion Rate: ${throughput.toLocaleString()} jobs/sec`);

  process.exit(0);
}

benchmark(10000);