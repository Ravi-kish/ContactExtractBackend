/**
 * Upload Queue — concurrency-limited async worker
 * Uses ingestion pipeline for all file processing
 */
import { logger } from '../config/logger';
import { runIngestionPipeline } from '../services/ingestionPipeline';

export interface UploadJobData {
  uploadId: string;
  filePath: string;
  fileName: string;
}

// Concurrency limiter — max N files processed simultaneously
const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT_UPLOADS || '3', 10);
let activeJobs = 0;
const jobQueue: Array<() => void> = [];

function acquireSlot(): Promise<void> {
  return new Promise((resolve) => {
    if (activeJobs < MAX_CONCURRENT) {
      activeJobs++;
      resolve();
    } else {
      jobQueue.push(() => { activeJobs++; resolve(); });
    }
  });
}

function releaseSlot(): void {
  activeJobs--;
  const next = jobQueue.shift();
  if (next) next();
}

export async function enqueueFile(data: UploadJobData): Promise<void> {
  // Fire and forget with concurrency control
  setImmediate(async () => {
    await acquireSlot();
    try {
      const result = await runIngestionPipeline({
        uploadId: data.uploadId,
        filePath: data.filePath,
        fileName: data.fileName,
      });

      logger.info(
        `[Queue] Job done: ${result.fileName} | ` +
        `${result.successRows} inserted | ${result.failedRows} failed | ` +
        `${result.durationMs}ms | ${result.throughputRowsPerSec} rows/sec | ` +
        `status: ${result.status}`
      );
    } catch (err) {
      logger.error(`[Queue] Unhandled job error for ${data.fileName}:`, err);
    } finally {
      releaseSlot();
    }
  });
}

export function startWorker() {
  logger.info(`[Queue] Worker ready (max ${MAX_CONCURRENT} concurrent uploads)`);
  return null;
}
