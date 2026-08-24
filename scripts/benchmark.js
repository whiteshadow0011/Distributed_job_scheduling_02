import axios from 'axios';

const BASE_URL = 'http://localhost:5000/api/v1';
const EMAIL = 'admin@acme.com';
const PASSWORD = 'securepassword123';

const TOTAL_JOBS = 50000;
const BATCH_SIZE = 500;

async function runBenchmark() {
  try {
    // 1. Authenticate to get fresh JWT token
    console.log('[Benchmark] Authenticating with API...');
    const loginRes = await axios.post(`${BASE_URL}/auth/login`, {
      email: EMAIL,
      password: PASSWORD,
    });

    const token = loginRes.data.token || loginRes.data.accessToken;
    if (!token) {
      throw new Error(`Login failed to return token. Response: ${JSON.stringify(loginRes.data)}`);
    }

    // 2. Fetch Queues
    const queueRes = await axios.get(`${BASE_URL}/queues`, {
      headers: { Authorization: `Bearer ${token}` },
    });

    const queues = Array.isArray(queueRes.data) ? queueRes.data : queueRes.data.queues;
    if (!queues || queues.length === 0) {
      throw new Error('No queues found for this organization.');
    }

    const queueId = queues[0].id;
    const queueName = queues[0].name;

    console.log(`[Benchmark] Authenticated! Target Queue: "${queueName}" (${queueId})`);
    console.log(`[Benchmark] Ingesting ${TOTAL_JOBS} jobs in batches of ${BATCH_SIZE}...`);

    const startTime = Date.now();

    // 3. Batch Ingestion Loop
    for (let i = 0; i < TOTAL_JOBS; i += BATCH_SIZE) {
      const jobs = Array.from({ length: BATCH_SIZE }, (_, idx) => ({
        type: 'SEND_WELCOME_EMAIL',
        payload: { email: `load_user_${i + idx}@scale.io` },
        priority: Math.floor(Math.random() * 10) + 1,
      }));

      await axios.post(
        `${BASE_URL}/jobs/batch`,
        { queueId, jobs },
        { headers: { Authorization: `Bearer ${token}` } }
      );

      const progress = Math.min(i + BATCH_SIZE, TOTAL_JOBS);
      process.stdout.write(`\r[Ingestion Progress]: ${progress} / ${TOTAL_JOBS} jobs enqueued.`);
    }

    const durationSec = ((Date.now() - startTime) / 1000).toFixed(2);
    const throughput = Math.round(TOTAL_JOBS / durationSec);

    console.log(`\n[Benchmark Done] Ingested ${TOTAL_JOBS} jobs in ${durationSec}s (~${throughput} jobs/sec ingestion rate).`);
  } catch (error) {
    if (error.response) {
      console.error('\n[API Error]:', error.response.status, error.response.data);
    } else {
      console.error('\n[Execution Error]:', error.message);
    }
  }
}

runBenchmark();