import Redis from 'ioredis';
import dotenv from 'dotenv';

dotenv.config();

export const redis = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
  retryStrategy(times) {
    const delay = Math.min(times * 50, 2000);
    return delay;
  },
});

redis.on('connect', () => {
  console.log('[Redis] Connected to Redis server successfully.');
});

redis.on('error', (err) => {
  console.error('[Redis Error]:', err.message);
});