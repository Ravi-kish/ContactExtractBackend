import fs from 'fs';
import path from 'path';
import Papa from 'papaparse';
import * as XLSX from 'xlsx';
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
  raw_row_json?: Record<string, unknown>;
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

function transformRecord(mapped: Record<string, unknown>, unmapped: Record<string, unknown>): ParsedRecord {
  const cdrNumber = normalizeText(mapped.cdr_number);
  const bParty = normalizeText(mapped.b_party);

  const callDateRaw = normalizeText(mapped.call_date);
  const callTimeRaw = normalizeText(mapped.call_time);
  const callDate = parseDate(callDateRaw);
  const callTime = parseTime(callTimeRaw);
  const callDatetime = combineDatetime(callDate, callTime);

  const lat = mapped.latitude ? parseFloat(String(mapped.latitude)) : null;
  const lon = mapped.longitude ? parseFloat(String(mapped.longitude)) : null;

  let roaming: boolean | null = null;
  if (mapped.roaming !== undefined && mapped.roaming !== null) {
    const r = String(mapped.roaming).toLowerCase();
    roaming = r === 'true' || r === '1' || r === 'yes' || r === 'y';
  }

  return {
    cdr_number: cdrNumber,
    cdr_number_e164: normalizePhone(cdrNumber),
    b_party: bParty,
    b_party_e164: normalizePhone(bParty),
    name_b_party: normalizeText(mapped.name_b_party),
    father_name: normalizeText(mapped.father_name),
    permanent_address: normalizeText(mapped.permanent_address),
    call_date: callDate ? callDate.toISOString().split('T')[0] : null,
    call_datetime_utc: callDatetime
      ? callDatetime.toISOString().replace('T', ' ').replace('Z', '').split('.')[0]
      : null,
    duration_seconds: parseDuration(mapped.duration_seconds as string | number | null),
    call_type: normalizeText(mapped.call_type),
    first_cell_id: normalizeText(mapped.first_cell_id),
    first_cell_address: normalizeText(mapped.first_cell_address),
    last_cell_id: normalizeText(mapped.last_cell_id),
    last_cell_address: normalizeText(mapped.last_cell_address),
    imei: normalizeText(mapped.imei),
    imsi: normalizeText(mapped.imsi),
    roaming,
    circle: normalizeText(mapped.circle),
    operator: normalizeText(mapped.operator),
    main_city: normalizeText(mapped.main_city),
    sub_city: normalizeText(mapped.sub_city),
    latitude: lat && !isNaN(lat) ? lat : null,
    longitude: lon && !isNaN(lon) ? lon : null,
    device_type: normalizeText(mapped.device_type, 500),
    device_manufacturer: normalizeText(mapped.device_manufacturer, 500),
    cdr_name: normalizeText(mapped.cdr_name, 200),
    cdr_address: normalizeText(mapped.cdr_address, 500),
    raw_row_json: Object.keys(unmapped).length > 0 ? unmapped : undefined,
  };
}

export async function parseCSV(filePath: string): Promise<ParseResult> {
  return new Promise((resolve) => {
    const content = fs.readFileSync(filePath, 'utf-8');
    const records: ParsedRecord[] = [];
    const errors: string[] = [];
    let totalRows = 0;
    let errorRows = 0;
    let columnMapping: Record<string, string> = {};
    let headersDetected = false;

    Papa.parse(content, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      step: (result) => {
        if (!headersDetected && result.meta.fields) {
          columnMapping = buildColumnMapping(result.meta.fields);
          headersDetected = true;
        }

        totalRows++;
        const rawRow = result.data as Record<string, unknown>;

        if (result.errors.length > 0) {
          errorRows++;
          errors.push(`Row ${totalRows}: ${result.errors[0].message}`);
          return;
        }

        const { mapped, unmapped } = mapRow(rawRow, columnMapping);

        if (!mapped.cdr_number && !mapped.b_party) {
          errorRows++;
          errors.push(`Row ${totalRows}: Missing critical fields (CDR Number and B Party)`);
          return;
        }

        records.push(transformRecord(mapped, unmapped));
      },
      complete: () => {
        resolve({ records, totalRows, errorRows, errors: errors.slice(0, 100) });
      },
      error: (err) => {
        logger.error('CSV parse error:', err);
        resolve({ records, totalRows, errorRows: totalRows, errors: [err.message] });
      },
    });
  });
}

export async function parseExcel(filePath: string): Promise<ParseResult> {
  const records: ParsedRecord[] = [];
  const errors: string[] = [];
  let totalRows = 0;
  let errorRows = 0;

  try {
    const workbook = XLSX.readFile(filePath, { cellDates: true, dense: false });
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];

    const rows = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, {
      defval: null,
      raw: false,
    });

    if (rows.length === 0) {
      return { records, totalRows: 0, errorRows: 0, errors: ['Empty sheet'] };
    }

    const headers = Object.keys(rows[0]);
    const columnMapping = buildColumnMapping(headers);

    for (const rawRow of rows) {
      totalRows++;
      const { mapped, unmapped } = mapRow(rawRow, columnMapping);

      if (!mapped.cdr_number && !mapped.b_party) {
        errorRows++;
        errors.push(`Row ${totalRows}: Missing critical fields`);
        continue;
      }

      records.push(transformRecord(mapped, unmapped));
    }
  } catch (err) {
    logger.error('Excel parse error:', err);
    errors.push(String(err));
    errorRows = totalRows;
  }

  return { records, totalRows, errorRows, errors: errors.slice(0, 100) };
}

export async function parseFile(filePath: string): Promise<ParseResult> {
  const ext = path.extname(filePath).toLowerCase();

  if (ext === '.csv') {
    return parseCSV(filePath);
  } else if (ext === '.xlsx' || ext === '.xls') {
    return parseExcel(filePath);
  } else {
    return { records: [], totalRows: 0, errorRows: 0, errors: [`Unsupported file type: ${ext}`] };
  }
}
