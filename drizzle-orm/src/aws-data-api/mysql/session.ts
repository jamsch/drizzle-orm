import type {
  ColumnMetadata,
  ExecuteStatementCommandOutput,
  Field,
  RDSDataClient,
} from "@aws-sdk/client-rds-data";
import {
  BeginTransactionCommand,
  CommitTransactionCommand,
  ExecuteStatementCommand,
  RollbackTransactionCommand,
} from "@aws-sdk/client-rds-data";
import { entityKind } from "~/entity.ts";
import { NoopLogger, type Logger } from "~/logger.ts";
import type {
  MySqlPreparedQueryHKT,
  MySqlDialect,
  MySqlQueryResultHKT,
  MySqlPreparedQueryConfig,
  PreparedQueryKind,
  SelectedFieldsOrdered,
} from "~/mysql-core/index.ts";
import {
  MySqlPreparedQuery,
  MySqlSession,
  MySqlTransaction,
} from "~/mysql-core/index.ts";
import type {
  RelationalSchemaConfig,
  TablesRelationalConfig,
} from "~/relations.ts";
import {
  fillPlaceholders,
  type SQL,
  type QueryTypingsValue,
  type QueryWithTypings,
  sql,
} from "~/sql/sql.ts";
import { type Assume, mapResultRow } from "~/utils.ts";
import { getValueFromDataApi, toValueParam } from "../common/index.ts";

export type AwsDataApiClient = RDSDataClient;

export class AwsDataApiPreparedQuery<
  T extends MySqlPreparedQueryConfig & {
    values: AwsDataApiMySqlQueryResult<unknown[]>;
  },
> extends MySqlPreparedQuery<T> {
  static override readonly [entityKind]: string =
    "AwsDataApiMySqlPreparedQuery";

  private rawQuery: ExecuteStatementCommand;

  constructor(
    private client: AwsDataApiClient,
    queryString: string,
    private params: unknown[],
    private typings: QueryTypingsValue[],
    private options: AwsDataApiSessionOptions,
    readonly transactionId: string | undefined,
    /*
    private options: AwsDataApiSessionOptions,
    private fields: SelectedFieldsOrdered | undefined,
        readonly transactionId: string | undefined,
    private _isResponseInArrayMode: boolean,
    private customResultMapper?: (rows: unknown[][]) => T["execute"]
    */
    private logger: Logger,
    private fields: SelectedFieldsOrdered | undefined,
    private customResultMapper?: (rows: unknown[][]) => T["execute"],
    // Keys that were used in $default and the value that was generated for them
    private generatedIds?: Record<string, unknown>[],
    // Keys that should be returned, it has the column with all properries + key from object
    private returningIds?: SelectedFieldsOrdered
  ) {
    super();
    this.rawQuery = new ExecuteStatementCommand({
      sql: queryString,
      parameters: [],
      secretArn: options.secretArn,
      resourceArn: options.resourceArn,
      database: options.database,
      transactionId,
      includeResultMetadata: !fields && !customResultMapper,
    });
  }

  async execute(
    placeholderValues: Record<string, unknown> | undefined = {}
  ): Promise<T["execute"]> {
    const { fields, joinsNotNullableMap, customResultMapper } = this;

    const result = await this.values(placeholderValues);
    if (!fields && !customResultMapper) {
      const { columnMetadata, rows } = result;
      if (!columnMetadata) {
        return result;
      }
      const mappedRows = rows.map((sourceRow) => {
        const row: Record<string, unknown> = {};
        for (const [index, value] of sourceRow.entries()) {
          const metadata = columnMetadata[index];
          if (!metadata) {
            throw new Error(
              `Unexpected state: no column metadata found for index ${index}. Please report this issue on GitHub: https://github.com/drizzle-team/drizzle-orm/issues/new/choose`
            );
          }
          if (!metadata.name) {
            throw new Error(
              `Unexpected state: no column name for index ${index} found in the column metadata. Please report this issue on GitHub: https://github.com/drizzle-team/drizzle-orm/issues/new/choose`
            );
          }
          row[metadata.name] = value;
        }
        return row;
      });
      return Object.assign(result, { rows: mappedRows });
    }

    return customResultMapper
      ? customResultMapper(result.rows!)
      : result.rows!.map((row) =>
          mapResultRow(fields!, row, joinsNotNullableMap)
        );
  }

  async values(
    placeholderValues: Record<string, unknown> = {}
  ): Promise<T["values"]> {
    const params = fillPlaceholders(this.params, placeholderValues ?? {});

    this.rawQuery.input.parameters = params.map((param, index) => ({
      name: `${index + 1}`,
      ...toValueParam(param, this.typings[index]),
    }));

    this.options.logger?.logQuery(
      this.rawQuery.input.sql!,
      this.rawQuery.input.parameters
    );

    const result = await this.client.send(this.rawQuery);
    const rows =
      result.records?.map((row) => {
        return row.map((field) => getValueFromDataApi(field));
      }) ?? [];

    return {
      ...result,
      rows,
    };
  }

  /** @internal */
  mapResultRows(records: Field[][], columnMetadata: ColumnMetadata[]) {
    return records.map((record) => {
      const row: Record<string, unknown> = {};
      for (const [index, field] of record.entries()) {
        const meta = columnMetadata[index as keyof typeof columnMetadata];
        const name =
          typeof meta === "object" && "name" in meta ? meta.name : undefined;

        row[name ?? index] = getValueFromDataApi(field); // not what to default if name is undefined
      }
      return row;
    });
  }

  override iterator(
    _placeholderValues?: Record<string, unknown>
  ): AsyncGenerator<T["iterator"]> {
    throw new Error("Streaming is not supported by the AWS Data API driver");
  }
}

export interface AwsDataApiSessionOptions {
  logger?: Logger;
  database: string;
  resourceArn: string;
  secretArn: string;
}

interface AwsDataApiQueryBase {
  resourceArn: string;
  secretArn: string;
  database: string;
}

export class AwsDataApiSession<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> extends MySqlSession<
  AwsDataApiMySqlQueryResultHKT,
  AwsDataApiMySqlPreparedQueryHKT,
  TFullSchema,
  TSchema
> {
  static override readonly [entityKind]: string = "AwsDataApiSession";

  private logger: Logger;

  /** @internal */
  readonly rawQuery: AwsDataApiQueryBase;

  constructor(
    /** @internal */
    readonly client: AwsDataApiClient,
    dialect: MySqlDialect,
    private schema: RelationalSchemaConfig<TSchema> | undefined,
    private options: AwsDataApiSessionOptions,
    /** @internal */
    readonly transactionId: string | undefined
  ) {
    super(dialect);
    this.rawQuery = {
      secretArn: options.secretArn,
      resourceArn: options.resourceArn,
      database: options.database,
    };
    this.logger = options.logger ?? new NoopLogger();
  }

  prepareQuery<
    T extends MySqlPreparedQueryConfig,
    TPreparedQueryHKT extends MySqlPreparedQueryHKT,
  >(
    query: QueryWithTypings,
    fields: SelectedFieldsOrdered | undefined,
    customResultMapper?: (rows: unknown[][]) => T["execute"],
    generatedIds?: Record<string, unknown>[],
    returningIds?: SelectedFieldsOrdered
  ): PreparedQueryKind<TPreparedQueryHKT, T> {
    return new AwsDataApiPreparedQuery(
      this.client,
      query.sql,
      query.params,
      query.typings ?? [],
      this.options,
      this.transactionId,
      this.logger,
      fields,
      customResultMapper,
      generatedIds,
      returningIds
    );
  }

  override execute<T>(query: SQL): Promise<T> {
    // @ts-expect-error - TODO: fix this
    return this.prepareQuery<T>(
      this.dialect.sqlToQuery(query),
      undefined,
      undefined,
      false,
      undefined,
      this.transactionId
    ).execute();
  }

  async all<T = unknown>(_query: SQL): Promise<T[]> {
    /*
    const result = await this.execute(query);
    if (!this.fields && !this.customResultMapper) {
      return (result as AwsDataApiMySqlQueryResult<unknown>).rows;
    }
    return result;
    */
    throw new Error("Method not implemented.");
  }

  override async transaction<T>(
    transaction: (tx: AwsDataApiTransaction<TFullSchema, TSchema>) => Promise<T>
    // config?: MySqlTransactionConfig | undefined
  ): Promise<T> {
    const { transactionId } = await this.client.send(
      new BeginTransactionCommand(this.rawQuery)
    );
    const session = new AwsDataApiSession(
      this.client,
      this.dialect,
      this.schema,
      this.options,
      transactionId
    );
    const tx = new AwsDataApiTransaction<TFullSchema, TSchema>(
      this.dialect,
      session as unknown as MySqlSession<any, any, any, any>,
      this.schema
    );
    /*
    if (config) {
      await tx.setTransaction(config);
    }
    */
    try {
      const result = await transaction(tx);
      await this.client.send(
        new CommitTransactionCommand({ ...this.rawQuery, transactionId })
      );
      return result;
    } catch (e) {
      await this.client.send(
        new RollbackTransactionCommand({ ...this.rawQuery, transactionId })
      );
      throw e;
    }
  }
}

export class AwsDataApiTransaction<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> extends MySqlTransaction<
  AwsDataApiMySqlQueryResultHKT,
  AwsDataApiMySqlPreparedQueryHKT,
  TFullSchema,
  TSchema
> {
  static override readonly [entityKind]: string = "AwsDataApiTransaction";

  constructor(
    dialect: MySqlDialect,
    session: MySqlSession,
    schema: RelationalSchemaConfig<TSchema> | undefined,
    nestedIndex = 0
  ) {
    super(dialect, session, schema, nestedIndex, "default");
  }

  override async transaction<T>(
    transaction: (tx: AwsDataApiTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    const savepointName = `sp${this.nestedIndex + 1}`;
    const tx = new AwsDataApiTransaction<TFullSchema, TSchema>(
      this.dialect,
      this.session,
      this.schema,
      this.nestedIndex + 1
    );
    await this.session.execute(sql.raw(`savepoint ${savepointName}`));
    try {
      const result = await transaction(tx);
      await this.session.execute(sql.raw(`release savepoint ${savepointName}`));
      return result;
    } catch (e) {
      await this.session.execute(
        sql.raw(`rollback to savepoint ${savepointName}`)
      );
      throw e;
    }
  }
}

export type AwsDataApiMySqlQueryResult<T> = ExecuteStatementCommandOutput & {
  rows: T[];
};

export interface AwsDataApiMySqlQueryResultHKT extends MySqlQueryResultHKT {
  type: AwsDataApiMySqlQueryResult<any>;
}

export interface AwsDataApiMySqlPreparedQueryHKT extends MySqlPreparedQueryHKT {
  type: AwsDataApiPreparedQuery<
    // @ts-expect-error - TODO: fix this
    Assume<this["config"], MySqlPreparedQueryConfig>
  >;
}
