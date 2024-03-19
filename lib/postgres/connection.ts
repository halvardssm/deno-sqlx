import {
  ArrayRow,
  ConnectionOptions,
  CoreConnectionWrapper,
  Row,
} from "../core/mod.ts";
import { Client, ClientOptions } from "@db/postgres";
import {
  PostgresTransaction,
  PostgresTransactionOptions,
} from "./transaction.ts";

/**
 * Type of the supported parameter types for the connection.
 */
export type PostgresConnectionParamType = unknown;

/**
 * Connection options for the PostgreSQL database.
 */
export interface PostgresConnectionOptions
  extends ClientOptions, ConnectionOptions {}

/**
 * Connection wrapper for the PostgreSQL database.
 */
export class PostgresConnection extends CoreConnectionWrapper<
  PostgresConnectionParamType,
  Client,
  ClientOptions,
  PostgresTransactionOptions,
  PostgresTransaction
> {
  constructor(connectionUrl: string, clientOptions: ClientOptions = {}) {
    super(connectionUrl, clientOptions);
  }

  async connect(): Promise<void> {
    this.client = new Client({
      ...PostgresConnection.parseConnectionUrl(this.connectionUrl),
      ...this.connectionOptions,
    });
    await this.client.connect();
  }

  async close(): Promise<void> {
    if (this._client) {
      await this.client.end();
      this.client = undefined;
    }
  }

  async execute(
    sql: string,
    params?: PostgresConnectionParamType[],
  ): Promise<number | undefined> {
    const res = await this.client.queryArray(sql, params);
    return res.rowCount;
  }

  async query<
    T extends Row<PostgresConnectionParamType> = Row<
      PostgresConnectionParamType
    >,
  >(
    sql: string,
    params?: PostgresConnectionParamType[],
  ): Promise<T[]> {
    const res = await this.client.queryObject<T>(sql, params);
    return res.rows;
  }

  async queryOne<
    T extends Row<PostgresConnectionParamType> = Row<
      PostgresConnectionParamType
    >,
  >(
    sql: string,
    params?: PostgresConnectionParamType[],
  ): Promise<T | undefined> {
    const res = await this.client.queryObject<T>(sql, params);
    return res.rows[0];
  }

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
  queryMany<
    T extends Row<PostgresConnectionParamType> = Row<
      PostgresConnectionParamType
    >,
  >(
    _sql: string,
    _params?: PostgresConnectionParamType[],
  ): Promise<AsyncIterable<T>> {
    return Promise.reject(new Error("Method not implemented."));
  }

  async queryArray<
    T extends ArrayRow<PostgresConnectionParamType> = ArrayRow<
      PostgresConnectionParamType
    >,
  >(
    sql: string,
    params?: PostgresConnectionParamType[],
  ): Promise<T[]> {
    const res = await this.client.queryArray<T>(sql, params);
    return res.rows;
  }

  async queryOneArray<
    T extends ArrayRow<PostgresConnectionParamType> = ArrayRow<
      PostgresConnectionParamType
    >,
  >(
    sql: string,
    params?: PostgresConnectionParamType[],
  ): Promise<T | undefined> {
    const res = await this.client.queryArray<T>(sql, params);
    return res.rows[0];
  }

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
  queryManyArray<
    T extends ArrayRow<PostgresConnectionParamType> = ArrayRow<
      PostgresConnectionParamType
    >,
  >(
    _sql: string,
    _params?: PostgresConnectionParamType[],
  ): Promise<AsyncIterable<T>> {
    return Promise.reject(new Error("Method not implemented."));
  }

  async beginTransaction(
    options: PostgresTransactionOptions["beginTransactionOptions"],
  ): Promise<PostgresTransaction> {
    const { name, ...rest } = options;
    const t = new PostgresTransaction(
      this.client.createTransaction(name, rest),
    );
    await t.client.begin();
    return t;
  }

  async transaction<T>(
    fn: (connection: PostgresTransaction) => Promise<T>,
  ): Promise<T> {
    const transaction = new PostgresTransaction(
      this.client.createTransaction(`transaction_${Math.random()}`),
    );
    await transaction.client.begin();
    const res = await fn(transaction);
    await transaction.commitTransaction();
    return res;
  }

  /**
   * Helpers
   */

  /**
   * @param connectionUrl [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...
   */
  static parseConnectionUrl(connectionUrl: string): ClientOptions {
    const url = new URL(connectionUrl);
    const urlParams = url.searchParams;

    const config: ClientOptions = {
      database: url.pathname.replace("/", "") || undefined,
      hostname: url.hostname,
      host_type: "tcp",
      password: url.password,
      options: [...urlParams.entries()].reduce((acc, [key, value]) => {
        acc[key] = value;
        return acc;
      }, {} as Record<string, string>),
      port: url.port ? parseInt(url.port) : undefined,
      user: url.username,
    };

    return config;
  }
}
