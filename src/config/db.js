import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;

// If DB_HOST is provided (e.g., 'postgres' in Docker), construct connection parameters
const isDocker = process.env.DB_HOST && process.env.DB_HOST !== 'localhost' && process.env.DB_HOST !== '127.0.0.1';

const poolConfig = isDocker
  ? {
      host: process.env.DB_HOST,
      port: parseInt(process.env.DB_PORT || '5432', 10),
      user: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgres',
      database: process.env.DB_NAME || 'scheduler_db',
    }
  : process.env.DATABASE_URL
  ? { connectionString: process.env.DATABASE_URL }
  : {
      host: 'localhost',
      port: 5432,
      user: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgres',
      database: process.env.DB_NAME || 'scheduler_db',
    };

export const pool = new Pool(poolConfig);

pool.on('connect', () => {
  console.log('[Database] Connected to PostgreSQL successfully.');
});

pool.on('error', (err) => {
  console.error('[Database] Unexpected error on idle client:', err);
});
