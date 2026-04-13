import type { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTableIfNotExists('cdr_records', (table) => {
    table.bigIncrements('id').primary();
    table.string('upload_id', 36).notNullable().references('id').inTable('uploads').onDelete('CASCADE');

    table.string('cdr_number', 30).nullable();
    table.string('cdr_number_e164', 20).nullable();
    table.string('b_party', 30).nullable();
    table.string('b_party_e164', 20).nullable();

    table.string('name_b_party', 200).nullable();
    table.string('father_name', 200).nullable();
    table.text('permanent_address').nullable();

    table.date('call_date').nullable();
    table.time('call_time').nullable();
    table.datetime('call_datetime_utc').nullable();
    table.integer('duration_seconds').nullable();
    table.string('call_type', 50).nullable();

    table.string('first_cell_id', 50).nullable();
    table.text('first_cell_address').nullable();
    table.string('last_cell_id', 50).nullable();
    table.text('last_cell_address').nullable();

    table.string('imei', 20).nullable();
    table.string('imsi', 20).nullable();
    table.boolean('roaming').nullable();

    table.string('circle', 100).nullable();
    table.string('operator', 100).nullable();
    table.string('main_city', 100).nullable();
    table.string('sub_city', 100).nullable();
    table.decimal('latitude', 10, 7).nullable();
    table.decimal('longitude', 10, 7).nullable();

    table.string('device_type', 100).nullable();
    table.string('device_manufacturer', 100).nullable();
    table.string('cdr_name', 200).nullable();
    table.text('cdr_address').nullable();

    table.json('raw_row_json').nullable();
    table.boolean('is_deleted').defaultTo(false);
    table.datetime('created_at').notNullable().defaultTo(knex.fn.now());

    // B-tree indexes
    table.index(['upload_id']);
    table.index(['cdr_number']);
    table.index(['cdr_number_e164']);
    table.index(['b_party']);
    table.index(['b_party_e164']);
    table.index(['call_date']);
    table.index(['call_datetime_utc']);
    table.index(['call_type']);
    table.index(['imei']);
    table.index(['imsi']);
    table.index(['circle']);
    table.index(['operator']);
    table.index(['main_city']);
    table.index(['first_cell_id']);
    table.index(['is_deleted']);
  });

  // Add FULLTEXT index only if it doesn't exist
  const [indexes] = await knex.raw(`SHOW INDEX FROM cdr_records WHERE Key_name = 'idx_cdr_fulltext'`);
  if (!indexes.length) {
    await knex.raw(`
      ALTER TABLE cdr_records ADD FULLTEXT INDEX idx_cdr_fulltext (
        cdr_number, b_party, name_b_party, father_name,
        permanent_address, imei, imsi, main_city, sub_city,
        operator, circle, first_cell_id, cdr_name
      )
    `);
  }
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('cdr_records');
}
