import { pool } from '../config/db.js';
import { calculateNextRetry } from './retry.js';

// Registry of job handlers
const taskHandlers = {
  SEND_WELCOME_EMAIL: async (payload) => {
    console.log(`[Task: SEND_WELCOME_EMAIL] Sending email to ${payload.email} (User #${payload.userId})...`);
    await new Promise((resolve) => setTimeout(resolve, 800)); // Simulate async network I/O
    return { delivered: true, timestamp: new Date().toISOString() };
  },

  GENERATE_INVOICE: async (payload) => {
    console.log(`[Task: GENERATE_INVOICE] Processing invoice #${payload.invoiceId}...`);
    await new Promise((resolve) => setTimeout(resolve, 1200)); // Simulate PDF rendering
    return { invoiceUrl: `https://storage.example.com/invoices/${payload.invoiceId}.pdf` };
  },

  SEND_REMINDER_SMS: async (payload) => {
    console.log(`[Task: SEND_REMINDER_SMS] Sending SMS to ${payload.phone}...`);
    await new Promise((resolve) => setTimeout(resolve, 500));
    return { sent: true };
  },

  FAILING_TASK_TEST: async () => {
    // For testing retry & DLQ logic
    throw new Error('Simulated network timeout error');
  },
};

/**
 * Executes a claimed job and handles completion or retry / DLQ states
 */
export async function executeJob(job, workerId) {
  const startTime = Date.now();
  const attemptNumber = job.retry_count + 1;

  try {
    const handler = taskHandlers[job.type];
    if (!handler) {
      throw new Error(`No registered handler for job type: ${job.type}`);
    }

    // Execute business task
    const output = await handler(job.payload);
    const durationMs = Date.now() - startTime;

    // Mark COMPLETED in jobs table and job_executions
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      await client.query(
        `UPDATE jobs 
         SET status = 'COMPLETED', locked_by_worker_id = NULL, locked_at = NULL, updated_at = NOW() 
         WHERE id = $1`,
        [job.id]
      );

      await client.query(
        `UPDATE job_executions 
         SET status = 'SUCCESS', duration_ms = $1, finished_at = NOW() 
         WHERE job_id = $2 AND attempt_number = $3`,
        [durationMs, job.id, attemptNumber]
      );

      await client.query('COMMIT');
      console.log(`[Worker] Job ${job.id} (${job.type}) COMPLETED in ${durationMs}ms`);
    } finally {
      client.release();
    }
  } catch (error) {
    const durationMs = Date.now() - startTime;
    console.error(`[Worker] Job ${job.id} FAILED (Attempt ${attemptNumber}):`, error.message);

    const retryPolicy = {
      strategy: job.strategy,
      max_retries: job.max_retries,
      base_delay_seconds: job.base_delay_seconds,
    };

    const { shouldRetry, nextRunAt } = calculateNextRetry(retryPolicy, job.retry_count);

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Update failed execution record
      await client.query(
        `UPDATE job_executions 
         SET status = 'FAILED', error_message = $1, duration_ms = $2, finished_at = NOW() 
         WHERE job_id = $3 AND attempt_number = $4`,
        [error.message, durationMs, job.id, attemptNumber]
      );

      if (shouldRetry) {
        // Queue again with backoff delay
        await client.query(
          `UPDATE jobs 
           SET 
             status = 'QUEUED', 
             retry_count = retry_count + 1, 
             run_at = $1, 
             locked_by_worker_id = NULL, 
             locked_at = NULL, 
             updated_at = NOW() 
           WHERE id = $2`,
          [nextRunAt, job.id]
        );
        console.log(`[Worker] Job ${job.id} rescheduled for retry at ${nextRunAt.toISOString()}`);
      } else {
        // Move to DLQ
        await client.query(
          `UPDATE jobs 
           SET status = 'DLQ', locked_by_worker_id = NULL, locked_at = NULL, updated_at = NOW() 
           WHERE id = $1`,
          [job.id]
        );

        await client.query(
          `INSERT INTO dead_letter_queue (job_id, queue_id, failed_reason)
           VALUES ($1, $2, $3)`,
          [job.id, job.queue_id, error.stack || error.message]
        );
        console.warn(`[Worker] Job ${job.id} moved to DEAD LETTER QUEUE (exhausted ${attemptNumber} attempts).`);
      }

      await client.query('COMMIT');
    } finally {
      client.release();
    }
  }
}