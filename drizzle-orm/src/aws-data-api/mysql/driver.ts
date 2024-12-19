import {
  RDSDataClient,
  type RDSDataClientConfig,
} from "@aws-sdk/client-rds-data";
import { entityKind } from "~/entity.ts";
import type { Logger } from "~/logger.ts";
import { DefaultLogger } from "~/logger.ts";
import {
  createTableRelationsHelpers,
  extractTablesRelationalConfig,
  type RelationalSchemaConfig,
  type TablesRelationalConfig,
} from "~/relations.ts";
import type { DrizzleConfig } from "~/utils.ts";
import type {
  AwsDataApiClient,
  AwsDataApiMySqlPreparedQueryHKT,
  AwsDataApiMySqlQueryResultHKT,
} from "./session.ts";
import { AwsDataApiSession } from "./session.ts";
import { MySqlDatabase } from "~/mysql-core/db.ts";
import { MySqlDialect } from "~/mysql-core/dialect.ts";

export interface MySqlDriverOptions {
  logger?: Logger;
  database: string;
  resourceArn: string;
  secretArn: string;
}

export interface DrizzleAwsDataApiMySqlConfig<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends DrizzleConfig<TSchema> {
  database: string;
  resourceArn: string;
  secretArn: string;
}

export class AwsDataApiMySqlDatabase<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends MySqlDatabase<
  AwsDataApiMySqlQueryResultHKT,
  AwsDataApiMySqlPreparedQueryHKT,
  TSchema
> {
  static override readonly [entityKind]: string = "AwsDataApiMySqlDatabase";

  /*
  override execute<
    TRow extends Record<string, unknown> = Record<string, unknown>,
  >(query: SQLWrapper | string): MySqlRaw<AwsDataApiMySqlQueryResult<TRow>> {
    return super.execute(query);
  }
	*/
}

export class AwsMySqlDialect extends MySqlDialect {
  static override readonly [entityKind]: string = "AwsMySqlDialect";

  override escapeParam(num: number): string {
    return `:${num + 1}`;
  }

  /*
  override buildInsertQuery({
    table,
    values,
    onConflict,
    returning,
    select,
  }: MySqlInsertConfig<MySqlTable<TableConfig>>): SQL<unknown> {
    const columns: Record<string, MySqlColumn> = table[Table.Symbol.Columns];

    if (!select) {
      for (const value of values as Record<string, Param | SQL>[]) {
        for (const fieldName of Object.keys(columns)) {
          const colValue = value[fieldName];
          if (
            is(colValue, Param) &&
            colValue.value !== undefined &&
            is(colValue.encoder, PgArray) &&
            Array.isArray(colValue.value)
          ) {
            value[fieldName] = sql`cast(${colValue} as ${sql.raw(
              colValue.encoder.getSQLType()
            )})`;
          }
        }
      }
    }

    return super.buildInsertQuery({
      table,
      values,
      onConflict,
      returning,
      withList,
    });
  }

  */

  /*
  override buildUpdateSet(
    table: PgTable<TableConfig>,
    set: UpdateSet
  ): SQL<unknown> {
    const columns: Record<string, PgColumn> = table[Table.Symbol.Columns];

    for (const [colName, colValue] of Object.entries(set)) {
      const currentColumn = columns[colName];
      if (
        currentColumn &&
        is(colValue, Param) &&
        colValue.value !== undefined &&
        is(colValue.encoder, PgArray) &&
        Array.isArray(colValue.value)
      ) {
        set[colName] = sql`cast(${colValue} as ${sql.raw(
          colValue.encoder.getSQLType()
        )})`;
      }
    }
    return super.buildUpdateSet(table, set);
  }
  */
}

function construct<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  client: AwsDataApiClient,
  config: DrizzleAwsDataApiMySqlConfig<TSchema>
): AwsDataApiMySqlDatabase<TSchema> & {
  $client: AwsDataApiClient;
} {
  const dialect = new AwsMySqlDialect({ casing: config.casing });
  let logger;
  if (config.logger === true) {
    logger = new DefaultLogger();
  } else if (config.logger !== false) {
    logger = config.logger;
  }

  let schema: RelationalSchemaConfig<TablesRelationalConfig> | undefined;
  if (config.schema) {
    const tablesConfig = extractTablesRelationalConfig(
      config.schema,
      createTableRelationsHelpers
    );
    schema = {
      fullSchema: config.schema,
      schema: tablesConfig.tables,
      tableNamesMap: tablesConfig.tableNamesMap,
    };
  }

  const session = new AwsDataApiSession(
    client,
    dialect,
    schema,
    { ...config, logger },
    undefined
  );
  const db = new AwsDataApiMySqlDatabase(
    dialect,
    session,
    schema as any,
    "default"
  );
  (<any>db).$client = client;

  return db as any;
}

export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
  TClient extends AwsDataApiClient = RDSDataClient,
>(
  ...params:
    | [TClient, DrizzleAwsDataApiMySqlConfig<TSchema>]
    | [
        | (DrizzleConfig<TSchema> & {
            connection: RDSDataClientConfig &
              Omit<DrizzleAwsDataApiMySqlConfig, keyof DrizzleConfig>;
          })
        | (DrizzleAwsDataApiMySqlConfig<TSchema> & {
            client: TClient;
          }),
      ]
): AwsDataApiMySqlDatabase<TSchema> & {
  $client: TClient;
} {
  // eslint-disable-next-line no-instanceof/no-instanceof
  if (params[0] instanceof RDSDataClient) {
    return construct(
      params[0] as TClient,
      params[1] as DrizzleAwsDataApiMySqlConfig<TSchema>
    ) as any;
  }

  if ((params[0] as { client?: TClient }).client) {
    const { client, ...drizzleConfig } = params[0] as {
      client: TClient;
    } & DrizzleAwsDataApiMySqlConfig<TSchema>;

    return construct(client, drizzleConfig) as any;
  }

  const { connection, ...drizzleConfig } = params[0] as {
    connection: RDSDataClientConfig &
      Omit<DrizzleAwsDataApiMySqlConfig, keyof DrizzleConfig>;
  } & DrizzleConfig<TSchema>;
  const { resourceArn, database, secretArn, ...rdsConfig } = connection;

  const instance = new RDSDataClient(rdsConfig);
  return construct(instance, {
    resourceArn,
    database,
    secretArn,
    ...drizzleConfig,
  }) as any;
}

export namespace drizzle {
  export function mock<
    TSchema extends Record<string, unknown> = Record<string, never>,
  >(
    config: DrizzleAwsDataApiMySqlConfig<TSchema>
  ): AwsDataApiMySqlDatabase<TSchema> & {
    $client: "$client is not available on drizzle.mock()";
  } {
    return construct({} as any, config) as any;
  }
}
