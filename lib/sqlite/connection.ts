// deno-lint-ignore-file require-await
import {
  ArrayRow,
  ConnectionOptions,
  CoreConnectionWrapper,
  Row,
  transformToAsyncIterable,
} from "../core/mod.ts";
import { BindValue, Database, DatabaseOpenOptions } from "@db/sqlite";
import { SqLiteTransaction } from "./transaction.ts";
import { SqLiteTransactionOptions } from "./transaction.ts";

export type SqLiteConnectionParamType = BindValue;

export interface SqLiteConnectionOptions
  extends DatabaseOpenOptions, ConnectionOptions {}

export class SqLiteConnection extends CoreConnectionWrapper<
  SqLiteConnectionParamType,
  Database,
  SqLiteConnectionOptions,
  SqLiteTransactionOptions,
  SqLiteTransaction
> {
  constructor(
    connectionUrl: string,
    connectionOptions: SqLiteConnectionOptions = {},
  ) {
    super(connectionUrl, connectionOptions);
  }
  async connect(): Promise<void> {
    this.client = new Database(this.connectionUrl, this.connectionOptions);
  }

  async close(): Promise<void> {
    if (this._client) {
      this.client.close();
      this.client = undefined;
    }
  }

  async execute(
    sql: string,
    params?: SqLiteConnectionParamType[],
  ): Promise<number | undefined> {
    return this.client.exec(sql, ...(params || []));
  }

  async query<
    T extends Row<SqLiteConnectionParamType> = Row<SqLiteConnectionParamType>,
  >(
    sql: string,
    params?: SqLiteConnectionParamType[],
  ): Promise<T[]> {
    const res = this.client.prepare(sql).all<T>(...(params || []));
    return res;
  }

  async queryOne<
    T extends Row<SqLiteConnectionParamType> = Row<SqLiteConnectionParamType>,
  >(
    sql: string,
    params?: SqLiteConnectionParamType[],
  ): Promise<T | undefined> {
    const res = this.client.prepare(sql).get<T>(...(params || []));
    return res;
  }

  async queryMany<
    T extends Row<SqLiteConnectionParamType> = Row<SqLiteConnectionParamType>,
  >(
    sql: string,
    params?: SqLiteConnectionParamType[] | undefined,
  ): Promise<AsyncIterable<T>> {
    return transformToAsyncIterable(
      this.client.prepare(sql).bind(...(params || []))[Symbol.iterator](),
    );
  }
  async queryArray<
    T extends ArrayRow<SqLiteConnectionParamType> = ArrayRow<
      SqLiteConnectionParamType
    >,
  >(
    sql: string,
    params?: SqLiteConnectionParamType[] | undefined,
  ): Promise<T[]> {
    const res = this.client.prepare(sql).values<T>(...(params || []));
    return res;
  }
  async queryOneArray<
    T extends ArrayRow<SqLiteConnectionParamType> = ArrayRow<
      SqLiteConnectionParamType
    >,
  >(
    sql: string,
    params?: SqLiteConnectionParamType[] | undefined,
  ): Promise<T | undefined> {
    const res = this.client.prepare(sql).value<T>(...(params || []));
    return res;
  }

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
  async queryManyArray<
    T extends ArrayRow<SqLiteConnectionParamType> = ArrayRow<
      SqLiteConnectionParamType
    >,
  >(
    _sql: string,
    _params?: SqLiteConnectionParamType[] | undefined,
  ): Promise<AsyncIterable<T>> {
    return Promise.reject(new Error("Method not implemented."));
  }

  async beginTransaction(
    options?: SqLiteTransactionOptions["beginTransactionOptions"],
  ): Promise<SqLiteTransaction> {
    let query = "BEGIN";
    if (options?.behavior) {
      query += ` ${options.behavior}`;
    }
    this.client.exec(query);

    return new SqLiteTransaction(this.client);
  }

  async transaction<T>(
    fn: (connection: SqLiteTransaction) => Promise<T>,
  ): Promise<T> {
    const res = this.client.transaction(() => {
      return fn(new SqLiteTransaction(this.client));
    });

    return res();
  }
}
