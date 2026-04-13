import type { Knex } from 'knex';
import bcrypt from 'bcryptjs';
import { v4 as uuidv4 } from 'uuid';

export async function seed(knex: Knex): Promise<void> {
  await knex('users').del();

  const username = process.env.SEED_USERNAME || 'admin';
  const password = process.env.SEED_PASSWORD || 'ChangeMe@123';

  await knex('users').insert([
    {
      id: uuidv4(),
      email: `${username}@ionora.local`,
      username,
      password_hash: await bcrypt.hash(password, 12),
      name: username,
      role: 'admin',
    },
  ]);

  console.log('✅ User seeded');
}
