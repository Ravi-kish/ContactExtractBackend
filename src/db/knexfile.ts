import type { Knex } from 'knex';
import path from 'path';
import dotenv from 'dotenv';

// Load .env from backend root — works regardless of cwd change by knex
const envPath = path.resolve(__dirname, '..', '..', '..', '.env');
dotenv.config({ path: envPath });

// Also try process.env directly (set by shell or npm scripts)
const connection = {
  host:     process.env.DB_HOST     || '127.0.0.1',
  port:     parseInt(process.env.DB_PORT || '3306', 10),
  user:     process.env.DB_USER     || 'root',
  password: process.env.DB_PASSWORD || 'root',
  database: process.env.DB_NAME     || 'cdr_db',
  timezone: '+00:00',
  charset:  'utf8mb4',
};

const config: { [key: string]: Knex.Config } = {
  development: {
    client: 'mysql2',
    connection,
    migrations: { directory: './migrations', extension: 'ts' },
    seeds:      { directory: './seeds',      extension: 'ts' },
  },
  production: {
    client: 'mysql2',
    connection,
    pool: { min: 2, max: 20 },
    migrations: { directory: './migrations' },
  },
};

export default config;
