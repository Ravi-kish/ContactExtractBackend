import fs from 'fs';
import path from 'path';
import { buildColumnMapping, mapRow } from '../utils/fieldMapper';
import { parseDate } from '../utils/dateParser';
import { logger } from '../config/logger';

export interface ParsedRecord {
  cdr_number:        string | null;
  b_party:           string | null;
  b_party_internal:  string | null;
  name_b_party:      string | null;
  father_name:       string | null;
  permanent_address: string | null;
  call_date:         string | null;
  main_city:         string | null;
  sub_city:          string | null;
}

export interface StreamParseOptions {
  onBatch: (records: ParsedRecord[], batchIndex: number) => Promise<void>;
  onProgress?: (processed: number, failed: number) => void;
  batchSize?: number;
}

export interface StreamParseResult {
  totalRows: number;
  successRows: number;
  failedRows: number;
  durationMs: number;
}

function normalizeText(val: unknown, maxLen?: number): string | null {
  if (val === null || val === undefined || val === '') return null;
  let str = String(val).normalize('NFC').trim();
  if (!str) return null;
  if (maxLen && str.length > maxLen) str = str.substring(0, maxLen);
  return str;
}

function transformRow(mapped: Record<string, unknown>): ParsedRecord {
  const callDate = parseDate(normalizeText(mapped.call_date));
  return {
    cdr_number:        normalizeText(mapped.cdr_number, 30),
    b_party:           normalizeText(mapped.b_party, 30),
    b_party_internal:  normalizeText(mapped.b_party_internal, 100),
    name_b_party:      normalizeText(mapped.name_b_party, 200),
    father_name:       normalizeText(mapped.father_name, 200),
    permanent_address: normalizeText(mapped.permanent_address),
    call_date:         callDate ? callDate.toISOString().split('T')[0] : null,
    main_city:         normalizeText(mapped.main_city, 100),
    sub_city:          normalizeText(mapped.sub_city, 100),
  };
}

function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = '', inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') { current += '"'; i++; }
      else inQuotes = !inQuotes;
    } else if (ch === ',' && !inQuotes) { result.push(current.trim()); current = ''; }
    else current += ch;
  }
  result.push(current.trim());
  return result;
}

export async function streamParseCSV(filePath: string, opts: StreamParseOptions): Promise<StreamParseResult> {
  const { onBatch, onProgress, batchSize = 500 } = opts;
  const startTime = Date.now();
  let totalRows = 0, successRows = 0, failedRows = 0, batchIndex = 0;
  let columnMapping: Record<string, string> | null = null;
  let headers: string[] = [];
  let batch: ParsedRecord[] = [];
  let remainder = '';

  const fileStream = fs.createReadStream(filePath, { encoding: 'utf8', highWaterMark: 64 * 1024 });

  for await (const chunk of fileStream) {
    const lines = (remainder + chunk).split('\n');
    remainder = lines.pop() || '';
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      if (!columnMapping) { headers = parseCSVLine(trimmed); columnMapping = buildColumnMapping(headers); continue; }
      totalRows++;
      const rawRow: Record<string, unknown> = {};
      parseCSVLine(trimmed).forEach((v, i) => { rawRow[headers[i]] = v || null; });
      batch.push(transformRow(mapRow(rawRow, columnMapping)));
      if (batch.length >= batchSize) {
        const cur = batch.splice(0);
        await onBatch(cur, batchIndex++);
        successRows += cur.length;
        onProgress?.(totalRows, failedRows);
      }
    }
  }

  if (batch.length > 0) { await onBatch(batch, batchIndex++); successRows += batch.length; }
  return { totalRows, successRows, failedRows, durationMs: Date.now() - startTime };
}

export async function streamParseExcel(filePath: string, opts: StreamParseOptions): Promise<StreamParseResult> {
  const { onBatch, onProgress, batchSize = 500 } = opts;
  const startTime = Date.now();
  let totalRows = 0, successRows = 0, failedRows = 0, batchIndex = 0;

  const ExcelJS = await import('exceljs');
  const workbook = new ExcelJS.default.Workbook();
  await workbook.xlsx.readFile(filePath);
  const worksheet = workbook.worksheets[0];
  if (!worksheet) return { totalRows: 0, successRows: 0, failedRows: 0, durationMs: Date.now() - startTime };

  let columnMapping: Record<string, string> | null = null;
  let headers: string[] = [];
  const batch: ParsedRecord[] = [];

  worksheet.eachRow({ includeEmpty: false }, (row) => {
    const cells = (row.values as unknown[]).slice(1).map(v => v === null || v === undefined ? '' : String(v).trim());
    if (!columnMapping) { headers = cells; columnMapping = buildColumnMapping(headers); return; }
    totalRows++;
    const rawRow: Record<string, unknown> = {};
    headers.forEach((h, i) => { rawRow[h] = cells[i] ?? null; });
    batch.push(transformRow(mapRow(rawRow, columnMapping)));
  });

  for (let i = 0; i < batch.length; i += batchSize) {
    const chunk = batch.slice(i, i + batchSize);
    try { await onBatch(chunk, batchIndex++); successRows += chunk.length; }
    catch (err) { failedRows += chunk.length; logger.error(`Batch ${batchIndex} failed:`, err); }
    onProgress?.(successRows + failedRows, failedRows);
  }

  return { totalRows, successRows, failedRows, durationMs: Date.now() - startTime };
}

export async function streamParseFile(filePath: string, opts: StreamParseOptions): Promise<StreamParseResult> {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.csv') return streamParseCSV(filePath, opts);
  if (ext === '.xlsx' || ext === '.xls') return streamParseExcel(filePath, opts);
  throw new Error(`Unsupported file type: ${ext}`);
}
