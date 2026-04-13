import { Router, Response } from 'express';
import { authenticate, AuthRequest } from '../middleware/auth';
import db from '../db/connection';
import { logger } from '../config/logger';
import * as XLSX from 'xlsx';

const router = Router();

const DEFAULT_COLUMNS = [
  'id', 'cdr_number', 'b_party', 'name_b_party', 'call_date', 'call_time',
  'call_datetime_utc', 'duration_seconds', 'call_type', 'imei', 'main_city',
  'operator', 'circle', 'first_cell_id', 'upload_id',
];

// Helper: case-insensitive LIKE for utf8mb4 without collation conflicts
function likeFilter(query: ReturnType<typeof db>, columns: string[], term: string) {
  return query.where((qb) => {
    columns.forEach((col, i) => {
      const method = i === 0 ? 'whereRaw' : 'orWhereRaw';
      qb[method](`\`${col}\` LIKE ?`, [`%${term}%`]);
    });
  });
}

// GET /api/search - Global keyword search
router.get('/', authenticate, async (req: AuthRequest, res: Response): Promise<void> => {
  const q = String(req.query.q || '').trim();
  const page = parseInt(String(req.query.page || '1'), 10);
  const limit = Math.min(parseInt(String(req.query.limit || '50'), 10), 500);
  const offset = (page - 1) * limit;

  if (!q) {
    res.status(400).json({ error: 'Query parameter q is required' });
    return;
  }

  try {
    const startTime = Date.now();
    const baseQuery = db('cdr_records');

    likeFilter(baseQuery, [
      'cdr_number', 'b_party', 'name_b_party', 'father_name',
      'imei', 'imsi', 'main_city', 'operator', 'circle',
      'permanent_address', 'first_cell_id', 'cdr_name',
      'cdr_number_e164', 'b_party_e164',
    ], q);

    const [records, countResult] = await Promise.all([
      baseQuery.clone().select(DEFAULT_COLUMNS).orderBy('call_datetime_utc', 'desc').limit(limit).offset(offset),
      baseQuery.clone().count('id as count').first(),
    ]);

    const elapsed = Date.now() - startTime;
    const total = parseInt(String(countResult?.count || '0'), 10);

    await db('audit_logs').insert({
      user_id: req.user!.id,
      action: 'SEARCH',
      query_string: q,
      result_count: total,
      metadata: JSON.stringify({ type: 'global', elapsed_ms: elapsed }),
    });

    res.json({
      data: records,
      pagination: { page, limit, total, pages: Math.ceil(total / limit) },
      meta: { query: q, elapsed_ms: elapsed },
    });
  } catch (err) {
    logger.error('Search error:', err);
    res.status(500).json({ error: 'Search failed' });
  }
});

// POST /api/search/advanced - Field-level search
router.post('/advanced', authenticate, async (req: AuthRequest, res: Response): Promise<void> => {
  const {
    cdr_number, b_party, name, father_name, city, imei, imsi,
    operator, circle, call_type, cell_id,
    date_from, date_to,
    page = 1, limit: rawLimit = 50,
  } = req.body;

  const limit = Math.min(parseInt(String(rawLimit), 10), 500);
  const offset = (parseInt(String(page), 10) - 1) * limit;

  try {
    const startTime = Date.now();
    const query = db('cdr_records');

    if (cdr_number) query.whereRaw('`cdr_number` LIKE ?', [`%${cdr_number}%`]);
    if (b_party) query.whereRaw('`b_party` LIKE ?', [`%${b_party}%`]);
    if (name) query.whereRaw('`name_b_party` LIKE ?', [`%${name}%`]);
    if (father_name) query.whereRaw('`father_name` LIKE ?', [`%${father_name}%`]);
    if (city) query.whereRaw('`main_city` LIKE ?', [`%${city}%`]);
    if (imei) query.whereRaw('`imei` LIKE ?', [`%${imei}%`]);
    if (imsi) query.whereRaw('`imsi` LIKE ?', [`%${imsi}%`]);
    if (operator) query.whereRaw('`operator` LIKE ?', [`%${operator}%`]);
    if (circle) query.whereRaw('`circle` LIKE ?', [`%${circle}%`]);
    if (call_type) query.whereRaw('`call_type` LIKE ?', [`%${call_type}%`]);
    if (cell_id) query.whereRaw('`first_cell_id` LIKE ?', [`%${cell_id}%`]);
    if (date_from) query.where('call_date', '>=', date_from);
    if (date_to) query.where('call_date', '<=', date_to);

    const [records, countResult] = await Promise.all([
      query.clone().select(DEFAULT_COLUMNS).orderBy('call_datetime_utc', 'desc').limit(limit).offset(offset),
      query.clone().count('id as count').first(),
    ]);

    const elapsed = Date.now() - startTime;
    const total = parseInt(String(countResult?.count || '0'), 10);

    await db('audit_logs').insert({
      user_id: req.user!.id,
      action: 'SEARCH',
      query_string: JSON.stringify(req.body),
      result_count: total,
      metadata: JSON.stringify({ type: 'advanced', elapsed_ms: elapsed }),
    });

    res.json({
      data: records,
      pagination: { page, limit, total, pages: Math.ceil(total / limit) },
      meta: { elapsed_ms: elapsed },
    });
  } catch (err) {
    logger.error('Advanced search error:', err);
    res.status(500).json({ error: 'Advanced search failed' });
  }
});

// GET /api/search/export - Export results
router.get('/export', authenticate, async (req: AuthRequest, res: Response): Promise<void> => {
  const q = String(req.query.q || '').trim();
  const format = String(req.query.format || 'csv').toLowerCase();

  try {
    const query = db('cdr_records').limit(10000);

    if (q) {
      likeFilter(query, ['cdr_number', 'b_party', 'name_b_party', 'imei', 'main_city'], q);
    }

    const records = await query.select(DEFAULT_COLUMNS).orderBy('call_datetime_utc', 'desc');

    if (format === 'xlsx') {
      const ws = XLSX.utils.json_to_sheet(records);
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, 'Records');
      const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', 'attachment; filename="cdr_export.xlsx"');
      res.send(buffer);
    } else {
      const headers = DEFAULT_COLUMNS.join(',');
      const rows = records.map((r) =>
        DEFAULT_COLUMNS.map((col) => {
          const val = (r as Record<string, unknown>)[col];
          if (val === null || val === undefined) return '';
          const str = String(val);
          return str.includes(',') || str.includes('"') ? `"${str.replace(/"/g, '""')}"` : str;
        }).join(',')
      );
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename="cdr_export.csv"');
      res.send([headers, ...rows].join('\n'));
    }
  } catch (err) {
    logger.error('Export error:', err);
    res.status(500).json({ error: 'Export failed' });
  }
});

export default router;
