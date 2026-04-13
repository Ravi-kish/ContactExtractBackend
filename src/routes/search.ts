import { Router, Response } from 'express';
import { authenticate, AuthRequest } from '../middleware/auth';
import db from '../db/connection';
import { logger } from '../config/logger';
import * as XLSX from 'xlsx';

const router = Router();

const DEFAULT_COLUMNS = [
  'id', 'cdr_number', 'b_party', 'b_party_internal',
  'name_b_party', 'father_name', 'permanent_address',
  'call_date', 'main_city', 'sub_city', 'upload_id',
];

function likeFilter(query: ReturnType<typeof db>, columns: string[], term: string) {
  return query.where((qb) => {
    columns.forEach((col, i) => {
      const method = i === 0 ? 'whereRaw' : 'orWhereRaw';
      qb[method](`\`${col}\` LIKE ?`, [`%${term}%`]);
    });
  });
}

// GET /api/search
router.get('/', authenticate, async (req: AuthRequest, res: Response): Promise<void> => {
  const q = String(req.query.q || '').trim();
  const page = parseInt(String(req.query.page || '1'), 10);
  const limit = Math.min(parseInt(String(req.query.limit || '50'), 10), 500);
  const offset = (page - 1) * limit;

  if (!q) { res.status(400).json({ error: 'Query parameter q is required' }); return; }

  try {
    const startTime = Date.now();
    const baseQuery = db('cdr_records');

    likeFilter(baseQuery, [
      'cdr_number', 'b_party', 'b_party_internal',
      'name_b_party', 'father_name', 'permanent_address',
      'main_city', 'sub_city',
    ], q);

    const [records, countResult] = await Promise.all([
      baseQuery.clone().select(DEFAULT_COLUMNS).orderBy('call_date', 'desc').limit(limit).offset(offset),
      baseQuery.clone().count('id as count').first(),
    ]);

    const elapsed = Date.now() - startTime;
    const total = parseInt(String(countResult?.count || '0'), 10);

    await db('audit_logs').insert({
      user_id: req.user!.id, action: 'SEARCH', query_string: q, result_count: total,
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

// POST /api/search/advanced
router.post('/advanced', authenticate, async (req: AuthRequest, res: Response): Promise<void> => {
  const {
    cdr_number, b_party, b_party_internal, name, father_name,
    permanent_address, city, sub_city, date_from, date_to,
    page = 1, limit: rawLimit = 50,
  } = req.body;

  const limit = Math.min(parseInt(String(rawLimit), 10), 500);
  const offset = (parseInt(String(page), 10) - 1) * limit;

  try {
    const startTime = Date.now();
    const query = db('cdr_records');

    if (cdr_number) query.whereRaw('`cdr_number` LIKE ?', [`%${cdr_number}%`]);
    if (b_party) query.whereRaw('`b_party` LIKE ?', [`%${b_party}%`]);
    if (b_party_internal) query.whereRaw('`b_party_internal` LIKE ?', [`%${b_party_internal}%`]);
    if (name) query.whereRaw('`name_b_party` LIKE ?', [`%${name}%`]);
    if (father_name) query.whereRaw('`father_name` LIKE ?', [`%${father_name}%`]);
    if (permanent_address) query.whereRaw('`permanent_address` LIKE ?', [`%${permanent_address}%`]);
    if (city) query.whereRaw('`main_city` LIKE ?', [`%${city}%`]);
    if (sub_city) query.whereRaw('`sub_city` LIKE ?', [`%${sub_city}%`]);
    if (date_from) query.where('call_date', '>=', date_from);
    if (date_to) query.where('call_date', '<=', date_to);

    const [records, countResult] = await Promise.all([
      query.clone().select(DEFAULT_COLUMNS).orderBy('call_date', 'desc').limit(limit).offset(offset),
      query.clone().count('id as count').first(),
    ]);

    const elapsed = Date.now() - startTime;
    const total = parseInt(String(countResult?.count || '0'), 10);

    await db('audit_logs').insert({
      user_id: req.user!.id, action: 'SEARCH',
      query_string: JSON.stringify(req.body), result_count: total,
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

// POST /api/search/export - Export with advanced filters
router.post('/export', authenticate, async (req: AuthRequest, res: Response): Promise<void> => {
  const {
    cdr_number, b_party, b_party_internal, name, father_name,
    permanent_address, city, sub_city, date_from, date_to, format = 'csv'
  } = req.body;

  try {
    const query = db('cdr_records').limit(10000);

    if (cdr_number) query.whereRaw('`cdr_number` LIKE ?', [`%${cdr_number}%`]);
    if (b_party) query.whereRaw('`b_party` LIKE ?', [`%${b_party}%`]);
    if (b_party_internal) query.whereRaw('`b_party_internal` LIKE ?', [`%${b_party_internal}%`]);
    if (name) query.whereRaw('`name_b_party` LIKE ?', [`%${name}%`]);
    if (father_name) query.whereRaw('`father_name` LIKE ?', [`%${father_name}%`]);
    if (permanent_address) query.whereRaw('`permanent_address` LIKE ?', [`%${permanent_address}%`]);
    if (city) query.whereRaw('`main_city` LIKE ?', [`%${city}%`]);
    if (sub_city) query.whereRaw('`sub_city` LIKE ?', [`%${sub_city}%`]);
    if (date_from) query.where('call_date', '>=', date_from);
    if (date_to) query.where('call_date', '<=', date_to);

    const records = await query.select(DEFAULT_COLUMNS).orderBy('call_date', 'desc');
    sendExport(res, records, format);
  } catch (err) {
    logger.error('Export error:', err);
    res.status(500).json({ error: 'Export failed' });
  }
});

// GET /api/search/export - Export results
router.get('/export', authenticate, async (req: AuthRequest, res: Response): Promise<void> => {
  const q = String(req.query.q || '').trim();
  const format = String(req.query.format || 'csv').toLowerCase();

  try {
    const query = db('cdr_records').limit(10000);
    if (q) likeFilter(query, ['cdr_number', 'b_party', 'name_b_party', 'main_city', 'sub_city'], q);
    const records = await query.select(DEFAULT_COLUMNS).orderBy('call_date', 'desc');
    sendExport(res, records, format);
  } catch (err) {
    logger.error('Export error:', err);
    res.status(500).json({ error: 'Export failed' });
  }
});

function sendExport(res: Response, records: Record<string, unknown>[], format: string): void {
  const exportHeaders = ['CdrNo', 'B Party', 'B Party Internal', 'Name B Party',
    'Father B Party', 'Permanent Address B Party', 'Date', 'Main City(First CellID)',
    'Sub City (First CellID)', 'Upload ID'];
  const exportCols = ['cdr_number', 'b_party', 'b_party_internal', 'name_b_party',
    'father_name', 'permanent_address', 'call_date', 'main_city', 'sub_city', 'upload_id'];

  if (format === 'xlsx') {
    const data = records.map(r => {
      const row: Record<string, unknown> = {};
      exportHeaders.forEach((h, i) => { row[h] = r[exportCols[i]] ?? ''; });
      return row;
    });
    const ws = XLSX.utils.json_to_sheet(data);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Records');
    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="ionora_export.xlsx"');
    res.send(buffer);
  } else {
    const rows = records.map(r =>
      exportCols.map(col => {
        const val = r[col];
        if (val === null || val === undefined) return '';
        const str = String(val);
        return str.includes(',') || str.includes('"') ? `"${str.replace(/"/g, '""')}"` : str;
      }).join(',')
    );
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="ionora_export.csv"');
    res.send([exportHeaders.join(','), ...rows].join('\n'));
  }
}

export default router;
