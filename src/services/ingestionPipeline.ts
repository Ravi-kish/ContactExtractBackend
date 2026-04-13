/**
 * Enterprise Ingestion Pipeline
 * - Streaming row-by-row processing (constant memory)
 * - Batch DB inserts with per-batch transactions
 * - Automatic file cleanup after processing
 * - Heap monitoring, backpressure, fault tolerance
 */
import fs from 'fs';
import path from 'path';
import os from 'os';
import db from '../db/connection';
import { streamParseFile, ParsedRecord } from './streamParser';
import { logger } from '../config/logger';

const BATCH_SIZE = parseInt(process.env.INGEST_BATCH_SIZE || '500', 10);
const MAX_HEAP_MB = parseInt(process.env.MAX_HEAP_MB || '512', 10);
const MAX_FILE_SIZE_MB = parseInt(process.env.MAX_FILE_SIZE_MB || '500', 10);
const MAX_RETRIES = 3;

export interface IngestionResult {
  uploadId: string;
  fileName: string;
  totalRows: number;
  successRows: number;
  failedRows: number;
  skippedRows: number;
  durationMs: number;
  throughputRowsPerSec: number;
  status: 'COMPLETE' | 'FAILED' | 'PARTIAL';
}

export interface IngestionOptions {
  uploadId: string;
  filePath: string;
  fileName: string;
  onProgress?: (processed: number, failed: number) => void;
}

// ─── Heap Monitor ────────────────────────────────────────────────────────────

function checkHeap(): void {
  const used = process.memoryUsage();
  const heapMB = Math.round(used.heapUsed / 1024 / 1024);
  if (heapMB > MAX_HEAP_MB) {
    logger.warn(`High heap usage: ${heapMB}MB / ${MAX_HEAP_MB}MB limit`);
    if (global.gc) {
      global.gc();
      logger.info('Forced GC triggered');
    }
  }
}

// ─── File Validation ─────────────────────────────────────────────────────────

function validateFile(filePath: string, fileName: string): void {
  // Path traversal protection
  const resolvedPath = path.resolve(filePath);
  const uploadsDir = path.resolve(process.env.UPLOAD_DIR || './uploads');
  if (!resolvedPath.startsWith(uploadsDir)) {
    throw new Error('Path traversal detected');
  }

  // Extension validation
  const ext = path.extname(fileName).toLowerCase();
  if (!['.csv', '.xlsx', '.xls'].includes(ext)) {
    throw new Error(`Unsupported file type: ${ext}`);
  }

  // File size check
  const stats = fs.statSync(filePath);
  const sizeMB = stats.size / (1024 * 1024);
  if (sizeMB > MAX_FILE_SIZE_MB) {
    throw new Error(`File too large: ${sizeMB.toFixed(1)}MB (max ${MAX_FILE_SIZE_MB}MB)`);
  }

  if (stats.size === 0) {
    throw new Error('File is empty');
  }
}

// ─── Batch DB Insert with Transaction ────────────────────────────────────────

async function insertBatch(
  records: ParsedRecord[],
  uploadId: string,
  batchIndex: number
): Promise<{ inserted: number; failed: number }> {
  const batchStart = Date.now();

  const rows = records.map(r => ({
    upload_id: uploadId,
    cdr_number: r.cdr_number ?? null,
    cdr_number_e164: r.cdr_number_e164 ?? null,
    b_party: r.b_party ?? null,
    b_party_e164: r.b_party_e164 ?? null,
    name_b_party: r.name_b_party ?? null,
    father_name: r.father_name ?? null,
    permanent_address: r.permanent_address ?? null,
    call_date: r.call_date ?? null,
    call_time: r.call_time ?? null,
    call_datetime_utc: r.call_datetime_utc ?? null,
    duration_seconds: r.duration_seconds ?? null,
    call_type: r.call_type ?? null,
    first_cell_id: r.first_cell_id ?? null,
    first_cell_address: r.first_cell_address ?? null,
    last_cell_id: r.last_cell_id ?? null,
    last_cell_address: r.last_cell_address ?? null,
    imei: r.imei ?? null,
    imsi: r.imsi ?? null,
    roaming: r.roaming ?? null,
    circle: r.circle ?? null,
    operator: r.operator ?? null,
    main_city: r.main_city ?? null,
    sub_city: r.sub_city ?? null,
    latitude: r.latitude ?? null,
    longitude: r.longitude ?? null,
    device_type: r.device_type ?? null,
    device_manufacturer: r.device_manufacturer ?? null,
    cdr_name: r.cdr_name ?? null,
    cdr_address: r.cdr_address ?? null,
    raw_row_json: r.raw_row_json ?? null,
  }));

  // Try with exponential backoff on transient errors
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      await db.transaction(async (trx) => {
        await trx('cdr_records').insert(rows);
      });
      const elapsed = Date.now() - batchStart;
      logger.info(`Batch ${batchIndex}: inserted ${rows.length} rows in ${elapsed}ms`);
      return { inserted: rows.length, failed: 0 };
    } catch (err: any) {
      const isTransient = err.code === 'ER_LOCK_DEADLOCK' ||
                          err.code === 'ER_LOCK_WAIT_TIMEOUT' ||
                          err.code === 'ECONNRESET';

      if (isTransient && attempt < MAX_RETRIES) {
        const delay = attempt * 500;
        logger.warn(`Batch ${batchIndex} attempt ${attempt} failed (${err.code}), retrying in ${delay}ms`);
        await new Promise(r => setTimeout(r, delay));
        continue;
      }

      // Packet too large — split batch in half and retry
      if (err.code === 'ER_NET_PACKET_TOO_LARGE' && rows.length > 1) {
        logger.warn(`Batch ${batchIndex} packet too large, splitting into halves`);
        const mid = Math.floor(rows.length / 2);
        const [r1, r2] = await Promise.all([
          insertBatch(records.slice(0, mid), uploadId, batchIndex * 100),
          insertBatch(records.slice(mid), uploadId, batchIndex * 100 + 1),
        ]);
        return { inserted: r1.inserted + r2.inserted, failed: r1.failed + r2.failed };
      }

      // Final fallback: row-by-row to salvage valid records
      logger.error(`Batch ${batchIndex} failed after ${attempt} attempts: ${err.message}`);
      let inserted = 0;
      let failed = 0;
      for (const row of rows) {
        try {
          await db('cdr_records').insert(row);
          inserted++;
        } catch {
          failed++;
        }
      }
      logger.info(`Batch ${batchIndex} fallback: ${inserted} saved, ${failed} failed`);
      return { inserted, failed };
    }
  }

  return { inserted: 0, failed: rows.length };
}

// ─── File Cleanup ─────────────────────────────────────────────────────────────

function cleanupFile(filePath: string): void {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      logger.info(`Cleaned up file: ${path.basename(filePath)}`);
    }
    // Also try to remove parent dir if empty
    const dir = path.dirname(filePath);
    const remaining = fs.readdirSync(dir);
    if (remaining.length === 0) {
      fs.rmdirSync(dir);
    }
  } catch (err) {
    logger.warn(`Cleanup failed for ${filePath}:`, err);
  }
}

// ─── Main Pipeline ────────────────────────────────────────────────────────────

export async function runIngestionPipeline(opts: IngestionOptions): Promise<IngestionResult> {
  const { uploadId, filePath, fileName, onProgress } = opts;
  const pipelineStart = Date.now();

  let totalInserted = 0;
  let totalFailed = 0;
  let batchCount = 0;

  logger.info(`[Pipeline] Starting: ${fileName} (upload: ${uploadId})`);

  try {
    // 1. Validate file
    validateFile(filePath, fileName);

    // 2. Update status to PROCESSING
    await db('uploads').where({ id: uploadId }).update({ status: 'PROCESSING' });

    // 3. Stream parse + batch insert
    const parseResult = await streamParseFile(filePath, {
      batchSize: BATCH_SIZE,

      onBatch: async (records, batchIndex) => {
        checkHeap();

        const { inserted, failed } = await insertBatch(records, uploadId, batchIndex);
        totalInserted += inserted;
        totalFailed += failed;
        batchCount++;

        // Update progress in DB every 10 batches
        if (batchCount % 10 === 0) {
          await db('uploads').where({ id: uploadId }).update({
            record_count: totalInserted,
            error_count: totalFailed,
          });
        }

        onProgress?.(totalInserted + totalFailed, totalFailed);
      },

      onProgress: (processed, failed) => {
        logger.debug(`[Pipeline] ${fileName}: ${processed} processed, ${failed} failed`);
      },
    });

    const durationMs = Date.now() - pipelineStart;
    const throughput = Math.round((parseResult.totalRows / durationMs) * 1000);

    const status: IngestionResult['status'] =
      totalFailed === 0 ? 'COMPLETE' :
      totalInserted > 0 ? 'PARTIAL' : 'FAILED';

    // 4. Final DB update — count actual inserted records
    const actualCount = await db('cdr_records').where({ upload_id: uploadId }).count('id as count').first();
    const finalCount = parseInt(String(actualCount?.count || totalInserted), 10);

    await db('uploads').where({ id: uploadId }).update({
      status,
      completed_at: db.fn.now(),
      record_count: finalCount,
      error_count: totalFailed,
    });

    logger.info(
      `[Pipeline] ✅ ${fileName}: ${totalInserted} inserted, ${totalFailed} failed, ` +
      `${durationMs}ms, ${throughput} rows/sec`
    );

    // 5. Delete file after successful processing
    cleanupFile(filePath);

    return {
      uploadId,
      fileName,
      totalRows: parseResult.totalRows,
      successRows: totalInserted,
      failedRows: totalFailed,
      skippedRows: parseResult.failedRows,
      durationMs,
      throughputRowsPerSec: throughput,
      status,
    };

  } catch (err: any) {
    const durationMs = Date.now() - pipelineStart;
    logger.error(`[Pipeline] ❌ ${fileName} failed: ${err.message}`);

    await db('uploads').where({ id: uploadId }).update({
      status: 'FAILED',
      completed_at: db.fn.now(),
      record_count: totalInserted,
      error_count: totalFailed,
    }).catch(() => {});

    // Always cleanup file even on failure
    cleanupFile(filePath);

    return {
      uploadId,
      fileName,
      totalRows: 0,
      successRows: totalInserted,
      failedRows: totalFailed,
      skippedRows: 0,
      durationMs,
      throughputRowsPerSec: 0,
      status: 'FAILED',
    };
  }
}

// ─── Scheduled Cleanup (orphaned files) ──────────────────────────────────────

export function scheduleOrphanCleanup(uploadDir: string, maxAgeHours = 24): void {
  const intervalMs = 60 * 60 * 1000; // every hour

  setInterval(() => {
    try {
      if (!fs.existsSync(uploadDir)) return;
      const entries = fs.readdirSync(uploadDir);
      const cutoff = Date.now() - maxAgeHours * 60 * 60 * 1000;

      for (const entry of entries) {
        const entryPath = path.join(uploadDir, entry);
        const stat = fs.statSync(entryPath);
        if (stat.mtimeMs < cutoff) {
          fs.rmSync(entryPath, { recursive: true, force: true });
          logger.info(`[Cleanup] Removed orphaned upload dir: ${entry}`);
        }
      }
    } catch (err) {
      logger.warn('[Cleanup] Orphan cleanup error:', err);
    }
  }, intervalMs);

  logger.info(`[Cleanup] Orphan file cleanup scheduled (every 1h, max age ${maxAgeHours}h)`);
}
