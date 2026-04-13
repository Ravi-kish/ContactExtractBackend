import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';
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

export interface ParseResult {
  records: ParsedRecord[];
  totalRows: number;
  errorRows: number;
  errors: string[];
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

export async function parseCSV(filePath: string): Promise<ParseResult> {
  return new Promise((resolve) => {
    const content = fs.readFileSync(filePath, 'utf-8');
    const records: ParsedRecord[] = [];
    const errors: string[] = [];
    let totalRows = 0, errorRows = 0;
    let columnMapping: Record<string, string> = {};
    let headersDetected = false;

    Papa.parse(content, {
      header: true,
      skipEmptyLines: true,
      step: (result) => {
        if (!headersDetected && result.meta.fields) {
          columnMapping = buildColumnMapping(result.meta.fields);
          headersDetected = true;
        }
        totalRows++;
        const mapped = mapRow(result.data as Record<string, unknown>, columnMapping);
        if (!mapped.cdr_number && !mapped.b_party) { errorRows++; return; }
        records.push(transformRow(mapped));
      },
      complete: () => resolve({ records, totalRows, errorRows, errors }),
      error: (err: Error) => resolve({ records, totalRows, errorRows: totalRows, errors: [err.message] }),
    });
  });
}

export async function parseExcel(filePath: string): Promise<ParseResult> {
  const records: ParsedRecord[] = [];
  const errors: string[] = [];
  let totalRows = 0, errorRows = 0;

  try {
    const ExcelJS = await import('exceljs');
    const workbook = new ExcelJS.default.Workbook();
    await workbook.xlsx.readFile(filePath);
    const worksheet = workbook.worksheets[0];
    if (!worksheet) return { records, totalRows: 0, errorRows: 0, errors: ['Empty sheet'] };

    let headers: string[] = [];
    let columnMapping: Record<string, string> = {};

    worksheet.eachRow({ includeEmpty: false }, (row) => {
      const cells = (row.values as unknown[]).slice(1).map(v =>
        v === null || v === undefined ? '' : String(v).trim()
      );
      if (headers.length === 0) { headers = cells; columnMapping = buildColumnMapping(headers); return; }
      totalRows++;
      const rawRow: Record<string, unknown> = {};
      headers.forEach((h, i) => { rawRow[h] = cells[i] ?? null; });
      const mapped = mapRow(rawRow, columnMapping);
      if (!mapped.cdr_number && !mapped.b_party) { errorRows++; return; }
      records.push(transformRow(mapped));
    });
  } catch (err) {
    logger.error('Excel parse error:', err);
    errors.push(String(err));
  }

  return { records, totalRows, errorRows, errors };
}

export async function parseFile(filePath: string): Promise<ParseResult> {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.csv') return parseCSV(filePath);
  if (ext === '.xlsx' || ext === '.xls') return parseExcel(filePath);
  return { records: [], totalRows: 0, errorRows: 0, errors: [`Unsupported: ${ext}`] };
}
