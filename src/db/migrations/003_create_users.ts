import type { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTableIfNotExists('users', (table) => {
    table.string('id', 36).primary();
    table.string('email', 255).notNullable().unique();
    table.string('password_hash', 255).notNullable();
    table.string('name', 200).notNullable();
    table.string('role', 50).defaultTo('analyst');
    table.boolean('is_active').defaultTo(true);
    table.datetime('created_at').notNullable().defaultTo(knex.fn.now());
    table.datetime('last_login').nullable();
  });

  await knex.schema.createTableIfNotExists('audit_logs', (table) => {
    table.bigIncrements('id').primary();
    table.string('user_id', 100).notNullable();
    table.string('action', 50).notNullable();
    table.text('query_string').nullable();
    table.integer('result_count').nullable();
    table.json('metadata').nullable();
    table.datetime('created_at').notNullable().defaultTo(knex.fn.now());

    table.index(['user_id']);
    table.index(['action']);
    table.index(['created_at']);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('audit_logs');
  await knex.schema.dropTableIfExists('users');
}
