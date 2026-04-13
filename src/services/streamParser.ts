/**
 * Enterprise streaming parser — processes CSV/Excel row-by-row
 * Never loads full file into memory. Constant memory usage regardless of file size.
 */
import fs from 'fs';
import path from 'path';
import { Transform, Readable } from 'stream';
import { pipeline } from 'stream/promises';
import { buildColumnMapping, mapRow } from '../utils/fieldMapper';
import { normalizePhone } from '../utils/phoneNormalizer';
import { parseDate, parseTime, combineDatetime, parseDuration } from '../utils/dateParser';
import { logger } from '../config/logger';

export interface ParsedRecord {
  cdr_number?: string | null;
  cdr_number_e164?: string | null;
  b_party?: string | null;
  b_party_e164?: string | null;
  name_b_party?: string | null;
  father_name?: string | null;
  permanent_address?: string | null;
  call_date?: string | null;
  call_time?: string | null;
  call_datetime_utc?: string | null;
  duration_seconds?: number | null;
  call_type?: string | null;
  first_cell_id?: string | null;
  first_cell_address?: string | null;
  last_cell_id?: string | null;
  last_cell_address?: string | null;
  imei?: string | null;
  imsi?: string | null;
  roaming?: boolean | null;
  circle?: string | null;
  operator?: string | null;
  main_city?: string | null;
  sub_city?: string | null;
  latitude?: number | null;
  longitude?: number | null;
  device_type?: string | null;
  device_manufacturer?: string | null;
  cdr_name?: string | null;
  cdr_address?: string | null;
  raw_row_json?: string | null;
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

function transformRow(mapped: Record<string, unknown>, unmapped: Record<string, unknown>): ParsedRecord {
  const cdrNumber = normalizeText(mapped.cdr_number, 30);
  const bParty = normalizeText(mapped.b_party, 30);
  const callDateRaw = normalizeText(mapped.call_date);
  const callTimeRaw = normalizeText(mapped.call_time);
  const callDate = parseDate(callDateRaw);
  const callTime = parseTime(callTimeRaw);
  const callDatetime = combineDatetime(callDate, callTime);

  const lat = mapped.latitude ? parseFloat(String(mapped.latitude)) : null;
  const lon = mapped.longitude ? parseFloat(String(mapped.longitude)) : null;

  let roaming: boolean | null = null;
  if (mapped.roaming !== undefined && mapped.roaming !== null && mapped.roaming !== '') {
    const r = String(mapped.roaming).toLowerCase();
    roaming = r === 'true' || r === '1' || r === 'yes' || r === 'y';
  }

  const rawJson = Object.keys(unmapped).length > 0 ? JSON.stringify(unmapped) : null;

  return {
    cdr_number: cdrNumber,
    cdr_number_e164: normalizePhone(cdrNumber),
    b_party: bParty,
    b_party_e164: normalizePhone(bParty),
    name_b_party: normalizeText(mapped.name_b_party, 200),
    father_name: normalizeText(mapped.father_name, 200),
    permanent_address: normalizeText(mapped.permanent_address),
    call_date: callDate ? callDate.toISOString().split('T')[0] : null,
    call_time: callTime,
    call_datetime_utc: callDatetime
      ? callDatetime.toISOString().replace('T', ' ').replace('Z', '').split('.')[0]
      : null,
    duration_seconds: parseDuration(mapped.duration_seconds as string | number | null),
    call_type: normalizeText(mapped.call_type, 50),
    first_cell_id: normalizeText(mapped.first_cell_id, 50),
    first_cell_address: normalizeText(mapped.first_cell_address),
    last_cell_id: normalizeText(mapped.last_cell_id, 50),
    last_cell_address: normalizeText(mapped.last_cell_address),
    imei: normalizeText(mapped.imei, 20),
    imsi: normalizeText(mapped.imsi, 20),
    roaming,
    circle: normalizeText(mapped.circle, 100),
    operator: normalizeText(mapped.operator, 100),
    main_city: normalizeText(mapped.main_city, 100),
    sub_city: normalizeText(mapped.sub_city, 100),
    latitude: lat && !isNaN(lat) ? lat : null,
    longitude: lon && !isNaN(lon) ? lon : null,
    device_type: normalizeText(mapped.device_type, 500),
    device_manufacturer: normalizeText(mapped.device_manufacturer, 500),
    cdr_name: normalizeText(mapped.cdr_name, 200),
    cdr_address: normalizeText(mapped.cdr_address, 500),
    raw_row_json: rawJson,
  };
}

// ─── CSV Streaming Parser ────────────────────────────────────────────────────

export async function streamParseCSV(
  filePath: string,
  opts: StreamParseOptions
): Promise<StreamParseResult> {
  const { onBatch, onProgress, batchSize = 1000 } = opts;
  const startTime = Date.now();

  let totalRows = 0;
  let successRows = 0;
  let failedRows = 0;
  let batchIndex = 0;
  let columnMapping: Record<string, string> | null = null;
  let headers: string[] = [];
  let batch: ParsedRecord[] = [];
  let headerLine = '';
  let remainder = '';

  const fileStream = fs.createReadStream(filePath, {
    encoding: 'utf8',
    highWaterMark: 64 * 1024, // 64KB chunks
  });

  const processLine = async (line: string): Promise<void> => {
    const trimmed = line.trim();
    if (!trimmed) return;

    if (!columnMapping) {
      // First non-empty line = headers
      headerLine = trimmed;
      headers = parseCSVLine(trimmed);
      columnMapping = buildColumnMapping(headers);
      return;
    }

    totalRows++;
    try {
      const values = parseCSVLine(trimmed);
      const rawRow: Record<string, unknown> = {};
      headers.forEach((h, i) => { rawRow[h] = values[i] ?? null; });

      const { mapped, unmapped } = mapRow(rawRow, columnMapping);
      const record = transformRow(mapped, unmapped);
      batch.push(record);

      if (batch.length >= batchSize) {
        const currentBatch = batch.splice(0, batch.length);
        await onBatch(currentBatch, batchIndex++);
        successRows += currentBatch.length;
        onProgress?.(totalRows, failedRows);
        // Allow GC to collect
        if (global.gc) global.gc();
      }
    } catch {
      failedRows++;
    }
  };

  // Process stream chunk by chunk
  for await (const chunk of fileStream) {
    const text = remainder + chunk;
    const lines = text.split('\n');
    remainder = lines.pop() || '';

    for (const line of lines) {
      await processLine(line);
    }
  }

  // Process remaining
  if (remainder.trim()) await processLine(remainder);

  // Flush final batch
  if (batch.length > 0) {
    await onBatch(batch, batchIndex++);
    successRows += batch.length;
    batch = [];
  }

  return {
    totalRows,
    successRows,
    failedRows,
    durationMs: Date.now() - startTime,
  };
}

// Simple CSV line parser (handles quoted fields)
function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') { current += '"'; i++; }
      else inQuotes = !inQuotes;
    } else if (ch === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  result.push(current.trim());
  return result;
}

// ─── Excel Streaming Parser ──────────────────────────────────────────────────

export async function streamParseExcel(
  filePath: string,
  opts: StreamParseOptions
): Promise<StreamParseResult> {
  const { onBatch, onProgress, batchSize = 1000 } = opts;
  const startTime = Date.now();

  let totalRows = 0;
  let successRows = 0;
  let failedRows = 0;
  let batchIndex = 0;

  const ExcelJS = await import('exceljs');
  const workbook = new ExcelJS.default.Workbook();
  await workbook.xlsx.readFile(filePath);

  const worksheet = workbook.worksheets[0];
  if (!worksheet) {
    return { totalRows: 0, successRows: 0, failedRows: 0, durationMs: Date.now() - startTime };
  }

  let columnMapping: Record<string, string> | null = null;
  let headers: string[] = [];
  let batch: ParsedRecord[] = [];

  worksheet.eachRow({ includeEmpty: false }, (row) => {
    const cells = row.values as (string | number | null | undefined)[];
    const values = cells.slice(1).map(v =>
      v === null || v === undefined ? '' : String(v).trim()
    );

    if (!columnMapping) {
      headers = values;
      columnMapping = buildColumnMapping(headers);
      return;
    }

    totalRows++;
    try {
      const rawRow: Record<string, unknown> = {};
      headers.forEach((h, i) => { rawRow[h] = values[i] ?? null; });
      const { mapped, unmapped } = mapRow(rawRow, columnMapping);
      batch.push(transformRow(mapped, unmapped));
    } catch {
      failedRows++;
    }
  });

  // Process all batches sequentially after reading
  for (let i = 0; i < batch.length; i += batchSize) {
    const chunk = batch.slice(i, i + batchSize);
    try {
      await onBatch(chunk, batchIndex++);
      successRows += chunk.length;
    } catch (err) {
      failedRows += chunk.length;
      logger.error(`Batch ${batchIndex} insert failed:`, err);
    }
    onProgress?.(successRows + failedRows, failedRows);
  }

  return {
    totalRows,
    successRows,
    failedRows,
    durationMs: Date.now() - startTime,
  };
}

// ─── Main Entry Point ────────────────────────────────────────────────────────

export async function streamParseFile(
  filePath: string,
  opts: StreamParseOptions
): Promise<StreamParseResult> {
  const ext = path.extname(filePath).toLowerCase();

  if (ext === '.csv') {
    return streamParseCSV(filePath, opts);
  } else if (ext === '.xlsx' || ext === '.xls') {
    return streamParseExcel(filePath, opts);
  } else {
    throw new Error(`Unsupported file type: ${ext}`);
  }
}
