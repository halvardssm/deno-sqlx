import {
  ArrayRow,
  ConnectionOptions,
  CoreConnectionWrapper,
  Param,
  Row,
} from "../core/mod.ts";
import { Client, ClientOptions } from "@db/postgres";
import { PostgresTransaction } from "./transaction.ts";

export interface PostgresConnectionOptions
  extends ClientOptions, ConnectionOptions {}

export class PostgresConnection
  extends CoreConnectionWrapper<Client, ClientOptions, PostgresTransaction> {
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

  async execute(sql: string, params?: Param[]): Promise<number | undefined> {
    const res = await this.client.queryArray(sql, params);
    return res.rowCount;
  }

  async query<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T[]> {
    const res = await this.client.queryObject<T>(sql, params);
    return res.rows;
  }

  async queryOne<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined> {
    const res = await this.client.queryObject<T>(sql, params);
    return res.rows[0];
  }

  queryMany<T extends Row = Row>(
    _sql: string,
    _params?: Param[],
  ): Promise<AsyncIterable<T>> {
    return Promise.reject(new Error("Method not implemented."));
  }

  async queryArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<T[]> {
    const res = await this.client.queryArray<T>(sql, params);
    return res.rows;
  }

  async queryOneArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined> {
    const res = await this.client.queryArray<T>(sql, params);
    return res.rows[0];
  }

  queryManyArray<T extends ArrayRow = ArrayRow>(
    _sql: string,
    _params?: Param[],
  ): Promise<AsyncIterable<T>> {
    return Promise.reject(new Error("Method not implemented."));
  }

  async transaction<T>(
    fn: (connection: PostgresTransaction) => Promise<T>,
  ): Promise<T> {
    const transaction = new PostgresTransaction(
      this.client.createTransaction(`transaction_${Math.random()}`),
    );
    await transaction.client.begin();
    const res = await fn(transaction);
    await transaction.commit();
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
