/**
 * Purge job: permanently deletes soft-deleted records older than 7 days.
 * Run as a cron job or scheduled task.
 */
import db from '../db/connection';
import { logger } from '../config/logger';

export async function purgeDeletedRecords(): Promise<void> {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 7);

  try {
    // Find uploads to purge
    const uploads = await db('uploads')
      .where('is_deleted', true)
      .where('deleted_at', '<', cutoff)
      .select('id');

    if (uploads.length === 0) {
      logger.info('Purge: no records to purge');
      return;
    }

    const ids = uploads.map(u => u.id);

    await db.transaction(async (trx) => {
      const deletedRecords = await trx('cdr_records').whereIn('upload_id', ids).delete();
      const deletedUploads = await trx('uploads').whereIn('id', ids).delete();
      logger.info(`Purge: deleted ${deletedUploads} uploads and ${deletedRecords} CDR records`);
    });
  } catch (err) {
    logger.error('Purge job failed:', err);
  }
}

// Run if called directly
if (require.main === module) {
  purgeDeletedRecords().then(() => process.exit(0));
}
