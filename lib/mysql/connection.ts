import {
  ArrayRow,
  ConnectionOptions,
  CoreConnectionWrapper,
  emptyAsyncIterable,
  Row,
} from "../core/mod.ts";
import { Client, ClientConfig } from "@db/mysql";
import { MySqlTransaction, MySqlTransactionOptions } from "./transaction.ts";

export type MySqlConnectionParamType = string;

export interface MySqlConnectionOptions
  extends ClientConfig, ConnectionOptions {
}

export class MySqlConnection extends CoreConnectionWrapper<
  MySqlConnectionParamType,
  Client,
  MySqlConnectionOptions,
  MySqlTransactionOptions,
  MySqlTransaction
> {
  constructor(
    connectionUrl: string,
    connectionOptions: MySqlConnectionOptions = {},
  ) {
    super(connectionUrl, connectionOptions);
  }

  async connect(): Promise<void> {
    this.client = await new Client().connect({
      ...MySqlConnection.parseConnectionUrl(this.connectionUrl),
      ...this.connectionOptions,
    });
  }

  async close(): Promise<void> {
    if (this._client) {
      await this.client.close();
      this.client = undefined;
    }
  }

  async execute(
    sql: string,
    params?: MySqlConnectionParamType[],
  ): Promise<number | undefined> {
    const res = await this.client.useConnection(async (connection) => {
      return await connection.execute(sql, params);
    });

    return res.affectedRows;
  }

  async query<
    T extends Row<MySqlConnectionParamType> = Row<MySqlConnectionParamType>,
  >(
    sql: string,
    params?: MySqlConnectionParamType[],
  ): Promise<T[]> {
    const res = await this.client.useConnection(async (connection) => {
      return await connection.execute(sql, params);
    });

    return (res.rows ?? []) as T[];
  }

  async queryOne<
    T extends Row<MySqlConnectionParamType> = Row<MySqlConnectionParamType>,
  >(
    sql: string,
    params?: MySqlConnectionParamType[],
  ): Promise<T | undefined> {
    const res = await this.client.useConnection(async (connection) => {
      return await connection.execute(sql, params);
    });

    return ((res.rows ?? []) as T[])[0];
  }

  async queryMany<
    T extends Row<MySqlConnectionParamType> = Row<MySqlConnectionParamType>,
  >(
    sql: string,
    params?: MySqlConnectionParamType[],
  ): Promise<AsyncIterable<T>> {
    const res = await this.client.useConnection(async (connection) => {
      return await connection.execute(sql, params, true);
    });
    return (res.iterator ?? emptyAsyncIterable()) as AsyncIterable<T>;
  }

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
  queryArray<
    T extends ArrayRow<MySqlConnectionParamType> = ArrayRow<
      MySqlConnectionParamType
    >,
  >(
    _sql: string,
    _params?: MySqlConnectionParamType[],
  ): Promise<T[]> {
    return Promise.reject(new Error("Method not implemented."));
  }

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
  queryOneArray<
    T extends ArrayRow<MySqlConnectionParamType> = ArrayRow<
      MySqlConnectionParamType
    >,
  >(
    _sql: string,
    _params?: MySqlConnectionParamType[],
  ): Promise<T | undefined> {
    return Promise.reject(new Error("Method not implemented."));
  }

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
  queryManyArray<
    T extends ArrayRow<MySqlConnectionParamType> = ArrayRow<
      MySqlConnectionParamType
    >,
  >(
    _sql: string,
    _params?: MySqlConnectionParamType[],
  ): Promise<AsyncIterable<T>> {
    return Promise.reject(new Error("Method not implemented."));
  }

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
  beginTransaction(
    _options?: MySqlTransactionOptions["beginTransactionOptions"],
  ): Promise<MySqlTransaction> {
    return Promise.reject(new Error("Method not implemented."));
  }

  transaction<T>(fn: (connection: MySqlTransaction) => Promise<T>): Promise<T> {
    return this.client.transaction<T>((conn) => fn(new MySqlTransaction(conn)));
  }

  /**
   * Helpers
   */

  /**
   * @param connectionUrl [scheme://][user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...
   */
  static parseConnectionUrl(connectionUrl: string): ClientConfig {
    const url = new URL(connectionUrl);

    const config: ClientConfig = {
      hostname: url.hostname || undefined,
      username: url.username || undefined,
      password: url.password || undefined,
      port: url.port ? parseInt(url.port) : undefined,
      db: url.pathname.replace("/", "") || undefined,
    };

    return config;
  }
}
