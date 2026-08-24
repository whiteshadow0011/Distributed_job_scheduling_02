import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { pool } from '../config/db.js';
import apiRoutes from './routes/apiRoutes.js';
import dlqRoutes from './routes/dlqRoutes.js'
import metricRoutes from './routes/metricRoutes.js'

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

app.use((req, res, next) => {
  console.log(`[API] ${req.method} ${req.originalUrl}`);
  next();
});

app.get('/api/health', async (req, res) => {
  try {
    const dbRes = await pool.query('SELECT NOW()');
    res.json({
      status: 'healthy',
      timestamp: dbRes.rows[0].now,
      service: 'distributed-job-scheduler-api',
    });
  } catch (error) {
    res.status(500).json({ status: 'unhealthy', error: error.message });
  }
});

// Mount Central API Routes
app.use('/api/v1', apiRoutes);

//Mount DLQ routes
app.use('/api/v1/queues/:queueId/dlq', dlqRoutes);

//Mount Metric Routes
app.use('/api/v1/metrics', metricRoutes);
app.use('/api/v1/queues/:queueId/metrics', metricRoutes);

app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint not found' });
});

app.use((err, req, res, next) => {
  console.error('[API Error]', err);
  res.status(err.status || 500).json({
    error: err.message || 'Internal Server Error',
  });
});

app.listen(PORT, async () => {
  console.log(`[Server] Express API server running on http://localhost:${PORT}`);
});