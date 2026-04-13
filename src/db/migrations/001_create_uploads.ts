import type { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTableIfNotExists('uploads', (table) => {
    table.string('id', 36).primary();
    table.datetime('created_at').notNullable().defaultTo(knex.fn.now());
    table.datetime('completed_at').nullable();
    table.integer('file_count').defaultTo(0);
    table.bigInteger('record_count').defaultTo(0);
    table.integer('error_count').defaultTo(0);
    table.string('status', 20).defaultTo('PENDING');
    table.string('uploader_id', 100).notNullable();
    table.text('notes').nullable();
    table.boolean('is_deleted').defaultTo(false);
    table.datetime('deleted_at').nullable();
    table.json('file_names').nullable();
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('uploads');
}
