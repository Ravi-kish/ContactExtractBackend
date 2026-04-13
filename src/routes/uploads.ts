import { Router, Response } from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { v4 as uuidv4 } from 'uuid';
import rateLimit from 'express-rate-limit';
import { authenticate, AuthRequest } from '../middleware/auth';
import db from '../db/connection';
import { enqueueFile } from '../queues/uploadQueue';
import { config } from '../config';
import { logger } from '../config/logger';

const router = Router();

// ─── Rate Limiting ────────────────────────────────────────────────────────────
const uploadRateLimit = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 20,
  message: { error: 'Too many upload requests, please try again later' },
});

// ─── Allowed Extensions & MIME Types ─────────────────────────────────────────
const ALLOWED_EXTENSIONS = ['.csv', '.xlsx', '.xls'];
const ALLOWED_MIMES = [
  'text/csv',
  'application/csv',
  'text/plain',
  'application/vnd.ms-excel',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'application/octet-stream',
];

// ─── Multer Storage ───────────────────────────────────────────────────────────
const storage = multer.diskStorage({
  destination: (req, _file, cb) => {
    const uploadId = (req as AuthRequest & { uploadId?: string }).uploadId || 'temp';
    // Sanitize uploadId — only allow UUID format
    const safeId = uploadId.replace(/[^a-zA-Z0-9\-]/g, '');
    const dir = path.join(config.uploadDir, safeId);
    fs.mkdirSync(dir, { recursive: true });
    cb(null, dir);
  },
  filename: (_req, file, cb) => {
    // Sanitize filename — strip path traversal and special chars
    const safeName = path.basename(file.originalname).replace(/[^a-zA-Z0-9._\-]/g, '_');
    cb(null, `${Date.now()}_${safeName}`);
  },
});

const upload = multer({
  storage,
  limits: {
    fileSize: config.maxFileSize,
    files: 500,
    fields: 10,
  },
  fileFilter: (_req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    const mime = file.mimetype.toLowerCase();

    // Validate extension
    if (!ALLOWED_EXTENSIONS.includes(ext)) {
      return cb(new Error(`Unsupported file type: ${ext}. Allowed: ${ALLOWED_EXTENSIONS.join(', ')}`));
    }

    // Validate MIME type
    if (!ALLOWED_MIMES.includes(mime)) {
      logger.warn(`Suspicious MIME type: ${mime} for file ${file.originalname}`);
      // Allow but log — some browsers send wrong MIME for CSV
    }

    cb(null, true);
  },
});

// Ensure upload dir exists
if (!fs.existsSync(config.uploadDir)) {
  fs.mkdirSync(config.uploadDir, { recursive: true });
}

// ─── POST /api/uploads — Create batch ────────────────────────────────────────
router.post('/', authenticate, uploadRateLimit, async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const uploadId = uuidv4();
    const notes = typeof req.body.notes === 'string'
      ? req.body.notes.substring(0, 500)  // sanitize length
      : null;

    await db('uploads').insert({
      id: uploadId,
      uploader_id: req.user!.id,
      status: 'PENDING',
      notes,
      file_names: JSON.stringify([]),
    });

    res.status(201).json({ upload_id: uploadId });
  } catch (err) {
    logger.error('Create upload error:', err);
    res.status(500).json({ error: 'Failed to create upload batch' });
  }
});

// ─── POST /api/uploads/:id/files — Upload files ───────────────────────────────
router.post(
  '/:id/files',
  authenticate,
  uploadRateLimit,
  (req: AuthRequest & { uploadId?: string }, _res, next) => {
    // Validate UUID format before using as directory name
    const id = req.params.id;
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
      _res.status(400).json({ error: 'Invalid upload ID format' });
      return;
    }
    req.uploadId = id;
    next();
  },
  upload.array('files', 500),
  async (req: AuthRequest, res: Response): Promise<void> => {
    const uploadId = req.params.id;

    try {
      const uploadBatch = await db('uploads').where({ id: uploadId }).first();
      if (!uploadBatch) {
        res.status(404).json({ error: 'Upload batch not found' });
        return;
      }

      const files = req.files as Express.Multer.File[];
      if (!files || files.length === 0) {
        res.status(400).json({ error: 'No valid files provided' });
        return;
      }

      // Update status
      await db('uploads').where({ id: uploadId }).update({
        status: 'PROCESSING',
        file_count: db.raw('file_count + ?', [files.length]),
        file_names: JSON.stringify(files.map(f => f.originalname)),
      });

      // Enqueue each file — non-blocking
      for (const file of files) {
        await enqueueFile({
          uploadId,
          filePath: file.path,
          fileName: file.originalname,
        });
      }

      logger.info(`[Upload] ${files.length} file(s) queued for upload ${uploadId}`);

      res.json({
        upload_id: uploadId,
        files_queued: files.length,
        message: 'Files queued for processing',
      });
    } catch (err) {
      logger.error('File upload error:', err);
      res.status(500).json({ error: 'Failed to process files' });
    }
  }
);

// ─── GET /api/uploads — List batches ─────────────────────────────────────────
router.get('/', authenticate, async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const page = Math.max(1, parseInt(String(req.query.page || '1'), 10));
    const limit = Math.min(100, parseInt(String(req.query.limit || '20'), 10));
    const offset = (page - 1) * limit;

    const [uploads, countResult] = await Promise.all([
      db('uploads').orderBy('created_at', 'desc').limit(limit).offset(offset),
      db('uploads').count('id as count').first(),
    ]);

    res.json({
      data: uploads,
      pagination: {
        page,
        limit,
        total: parseInt(String(countResult?.count || '0'), 10),
      },
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch uploads' });
  }
});

// ─── GET /api/uploads/:id — Single upload ────────────────────────────────────
router.get('/:id', authenticate, async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const upload = await db('uploads').where({ id: req.params.id }).first();
    if (!upload) { res.status(404).json({ error: 'Upload not found' }); return; }
    res.json(upload);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch upload' });
  }
});

// ─── GET /api/uploads/:id/preview — First 100 records ────────────────────────
router.get('/:id/preview', authenticate, async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const records = await db('cdr_records')
      .where({ upload_id: req.params.id })
      .limit(100)
      .select('id', 'cdr_number', 'b_party', 'name_b_party', 'call_date',
        'call_time', 'duration_seconds', 'call_type', 'imei', 'main_city', 'operator');
    res.json({ data: records, count: records.length });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch preview' });
  }
});

// ─── DELETE /api/uploads/:id — Hard delete everything ────────────────────────
router.delete('/:id', authenticate, async (req: AuthRequest, res: Response): Promise<void> => {
  try {
    const upload = await db('uploads').where({ id: req.params.id }).first();
    if (!upload) { res.status(404).json({ error: 'Upload not found' }); return; }

    await db.transaction(async (trx) => {
      await trx('cdr_records').where({ upload_id: req.params.id }).delete();
      await trx('uploads').where({ id: req.params.id }).delete();
      await trx('audit_logs').insert({
        user_id: req.user!.id,
        action: 'DELETE',
        metadata: JSON.stringify({
          upload_id: req.params.id,
          record_count: upload.record_count,
          deleted_at: new Date().toISOString(),
        }),
      });
    });

    // Delete physical files
    const uploadDir = path.join(config.uploadDir, req.params.id);
    if (fs.existsSync(uploadDir)) {
      fs.rmSync(uploadDir, { recursive: true, force: true });
    }

    logger.info(`[Delete] Upload ${req.params.id} permanently deleted by ${req.user!.id}`);
    res.json({ message: 'Upload and all records permanently deleted' });
  } catch (err) {
    logger.error('Delete error:', err);
    res.status(500).json({ error: 'Failed to delete upload' });
  }
});

export default router;
