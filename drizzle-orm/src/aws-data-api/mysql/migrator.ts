import type { MigrationConfig } from '~/migrator.ts';
import { readMigrationFiles } from '~/migrator.ts';
import type { AwsDataApiMySqlDatabase } from './driver.ts';

export async function migrate<TSchema extends Record<string, unknown>>(
	db: AwsDataApiMySqlDatabase<TSchema>,
	config: MigrationConfig,
) {
	const migrations = readMigrationFiles(config);
	await db.dialect.migrate(migrations, db.session, config);
}
