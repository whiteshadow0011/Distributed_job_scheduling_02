import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

pool.on('connect', () => {
  console.log('[Database] Connected to PostgreSQL successfully.');
});

pool.on('error', (err) => {
  console.error('[Database] Unexpected error on idle client:', err);
  process.exit(-1);
});