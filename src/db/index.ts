import { ConnectionString } from 'connection-string';
import SqliteDb from 'better-sqlite3'
import { Pool } from 'pg';
import { Kysely, Migrator, PostgresDialect, SqliteDialect } from 'kysely';
import { DatabaseSchema } from './schema'
import { migrationProvider } from './migrations'

export const createDb = (connectionString: string): Kysely<DatabaseSchema> => {
  if (connectionString === "sqlite://:memory:") {
    return new Kysely<DatabaseSchema>({
      dialect: new SqliteDialect({
        database: new SqliteDb(':memory:'),
      }),
    });
  }
  const c_string = new ConnectionString(connectionString);
  if (c_string.protocol === 'sqlite') {
    return new Kysely<DatabaseSchema>({
      dialect: new SqliteDialect({
        database: new SqliteDb(connectionString.replace('sqlite://', '')),
      }),
    });
  } else if (c_string.protocol === 'postgres') {
    return new Kysely<DatabaseSchema>({
      dialect: new PostgresDialect({
        pool: new Pool({
          host: c_string.hostname,
          database: c_string.path?.[0],
          user: c_string.user,
          password: c_string.password,
        }),
      }),
    });
  } else {
    throw new Error('Invalid connection string. Must start with "sqlite:" or "postgres:"');
  }
}

export const migrateToLatest = async (db: Database) => {
  const migrator = new Migrator({ db, provider: migrationProvider })
  const { error } = await migrator.migrateToLatest()
  if (error) throw error
}

export type Database = Kysely<DatabaseSchema>
