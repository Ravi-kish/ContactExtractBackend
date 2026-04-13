import { Router, Request, Response } from 'express';
import db from '../db/connection';

const router = Router();

router.get('/', async (_req: Request, res: Response): Promise<void> => {
  try {
    await db.raw('SELECT 1');
    res.json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      services: { database: 'up' },
    });
  } catch {
    res.status(503).json({
      status: 'unhealthy',
      timestamp: new Date().toISOString(),
      services: { database: 'down' },
    });
  }
});

export default router;
