import app from './app';
import { config } from './config';
import { logger } from './config/logger';
import { startWorker } from './queues/uploadQueue';
import { scheduleOrphanCleanup } from './services/ingestionPipeline';
import db from './db/connection';
import fs from 'fs';

async function bootstrap(): Promise<void> {
  // Ensure upload directory exists
  if (!fs.existsSync(config.uploadDir)) {
    fs.mkdirSync(config.uploadDir, { recursive: true });
  }

  // Ensure logs directory exists
  if (!fs.existsSync('logs')) {
    fs.mkdirSync('logs', { recursive: true });
  }

  // Test DB connection
  try {
    await db.raw('SELECT 1');
    logger.info('Database connected');
  } catch (err) {
    logger.error('Database connection failed — check DB env vars:', err);
    // Don't exit — let health endpoint report the issue
  }

  // Tables already created via schema.sql — skip migrations
  logger.info('Database schema ready');

  // Start worker and orphan cleanup
  startWorker();
  scheduleOrphanCleanup(config.uploadDir, 24);

  const port = parseInt(process.env.PORT || '3000', 10);
  const server = app.listen(port, '0.0.0.0', () => {
    logger.info(`CDR API server running on port ${port}`);
  });

  // Graceful shutdown
  const shutdown = async () => {
    logger.info('Shutting down...');
    server.close();
    await db.destroy();
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

bootstrap().catch((err) => {
  console.error('Bootstrap failed:', err);
  process.exit(1);
});
