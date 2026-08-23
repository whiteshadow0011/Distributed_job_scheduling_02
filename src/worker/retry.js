/**
 * Calculates next run_at timestamp based on retry policy strategy
 * @param {Object} policy - { strategy: 'FIXED'|'LINEAR'|'EXPONENTIAL', max_retries, base_delay_seconds }
 * @param {number} currentRetryCount
 * @returns {{ shouldRetry: boolean, nextRunAt: Date | null }}
 */
export function calculateNextRetry(policy, currentRetryCount) {
  const maxRetries = policy?.max_retries ?? 3;
  const baseDelay = policy?.base_delay_seconds ?? 5;
  const strategy = policy?.strategy || 'EXPONENTIAL';

  if (currentRetryCount >= maxRetries) {
    return { shouldRetry: false, nextRunAt: null };
  }

  let delaySeconds = baseDelay;

  switch (strategy) {
    case 'FIXED':
      delaySeconds = baseDelay;
      break;
    case 'LINEAR':
      delaySeconds = baseDelay * (currentRetryCount + 1);
      break;
    case 'EXPONENTIAL':
    default:
      // baseDelay * 2^(retryCount)
      delaySeconds = baseDelay * Math.pow(2, currentRetryCount);
      break;
  }

  const nextRunAt = new Date(Date.now() + delaySeconds * 1000);
  return { shouldRetry: true, nextRunAt };
}