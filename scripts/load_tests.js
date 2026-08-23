import dotenv from 'dotenv';
import { pool } from '../src/config/db.js';

dotenv.config();

const JOB_TYPES = [
  'SEND_WELCOME_EMAIL',
  'GENERATE_INVOICE',
  'SEND_REMINDER_SMS',
  'FAILING_TASK_TEST', // Will trigger backoff retries and DLQ
];

async function runLoadTest(count = 50) {
  try {
    // 1. Get an existing queue
    const queueRes = await pool.query('SELECT id, name FROM queues LIMIT 1');
    if (queueRes.rows.length === 0) {
      console.error('[Error] No queue found. Please create a queue first via the API.');
      process.exit(1);
    }

    const queue = queueRes.rows[0];
    console.log(`[Test] Ingesting ${count} jobs into queue: "${queue.name}" (${queue.id})...`);

    const insertValues = [];
    const placeholders = [];

    for (let i = 1; i <= count; i++) {
      const type = JOB_TYPES[Math.floor(Math.random() * JOB_TYPES.length)];
      const priority = Math.floor(Math.random() * 10) + 1; // Priority 1 to 10
      const payload = JSON.stringify({
        testId: i,
        email: `user_${i}@example.com`,
        invoiceId: 1000 + i,
        phone: `+1-555-${String(i).padStart(4, '0')}`,
      });

      const offset = (i - 1) * 4;
      placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, 'QUEUED', NOW())`);
      insertValues.push(queue.id, type, payload, priority);
    }

    const query = `
      INSERT INTO jobs (queue_id, type, payload, priority, status, run_at)
      VALUES ${placeholders.join(', ')}
      RETURNING id, type, priority;
    `;

    const result = await pool.query(query, insertValues);
    console.log(`[Success] Successfully queued ${result.rows.length} jobs with priorities 1-10.`);
    process.exit(0);
  } catch (error) {
    console.error('[Load Test Error]:', error);
    process.exit(1);
  }
}

runLoadTest(50);