import {
  ArrayRow,
  ConnectionOptions,
  CoreConnectionWrapper,
  emptyAsyncIterable,
  Param,
  Row,
} from "../core/mod.ts";
import { Client, ClientConfig } from "@db/mysql";
import { MySqlTransaction } from "./transaction.ts";

export interface MySqlConnectionOptions
  extends ClientConfig, ConnectionOptions {
}

export class MySqlConnection extends CoreConnectionWrapper<
  Client,
  MySqlConnectionOptions,
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

  async execute(sql: string, params?: Param[]): Promise<number | undefined> {
    const res = await this.client.useConnection(async (connection) => {
      return await connection.execute(sql, params);
    });

    return res.affectedRows;
  }

  async query<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T[]> {
    const res = await this.client.useConnection(async (connection) => {
      return await connection.execute(sql, params);
    });

    return (res.rows ?? []) as T[];
  }

  async queryOne<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined> {
    const res = await this.client.useConnection(async (connection) => {
      return await connection.execute(sql, params);
    });

    return ((res.rows ?? []) as T[])[0];
  }

  async queryMany<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<AsyncIterable<T>> {
    const res = await this.client.useConnection(async (connection) => {
      return await connection.execute(sql, params, true);
    });
    return (res.iterator ?? emptyAsyncIterable()) as AsyncIterable<T>;
  }

  queryArray<T extends ArrayRow = ArrayRow>(
    _sql: string,
    _params?: Param[],
  ): Promise<T[]> {
    return Promise.reject(new Error("Method not implemented."));
  }

  queryOneArray<T extends ArrayRow = ArrayRow>(
    _sql: string,
    _params?: Param[],
  ): Promise<T | undefined> {
    return Promise.reject(new Error("Method not implemented."));
  }

  queryManyArray<T extends ArrayRow = ArrayRow>(
    _sql: string,
    _params?: Param[],
  ): Promise<AsyncIterable<T>> {
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
