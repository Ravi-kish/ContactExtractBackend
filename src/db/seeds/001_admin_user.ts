import type { Knex } from 'knex';
import bcrypt from 'bcryptjs';
import { v4 as uuidv4 } from 'uuid';

export async function seed(knex: Knex): Promise<void> {
  await knex('users').del();

  await knex('users').insert([
    {
      id: uuidv4(),
      email: 'vamsi@ionora.local',
      username: 'U212521',
      password_hash: await bcrypt.hash('Vamsi$2125', 12),
      name: 'Vamsi',
      role: 'admin',
    },
  ]);

  console.log('✅ User seeded');
}
