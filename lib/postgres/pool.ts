import { ClientOptions, Pool, PoolClient } from "@db/postgres";
import {
  ArrayRow,
  ConnectionPoolOptions,
  CoreConnectionPoolWrapper,
  CorePoolConnectionWrapper,
  Param,
  Row,
} from "../core/mod.ts";
import { PostgresTransaction } from "./transaction.ts";
import { PostgresConnection } from "./connection.ts";

export interface PostgresPoolConnectionOptions
  extends ClientOptions, ConnectionPoolOptions {}
export class PostgresConnectionPool extends CoreConnectionPoolWrapper<
  Pool,
  PostgresPoolConnectionOptions,
  PostgresTransaction,
  PostgresPoolConnection
> {
  async acquire(): Promise<PostgresPoolConnection> {
    const conn = await this.client.connect();
    return new PostgresPoolConnection(conn);
  }
  connect(): Promise<void> {
    this.client = new Pool({
      ...PostgresConnection.parseConnectionUrl(this.connectionUrl),
      ...this.connectionOptions,
    }, this.poolSize);
    return Promise.resolve();
  }
  async close(): Promise<void> {
    if (this._client) {
      await this.client.end();
      this.client = undefined;
    }
  }

  async execute(sql: string, params?: Param[]): Promise<number | undefined> {
    await using client = await this.acquire();
    return client.execute(sql, params);
  }

  async query<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T[]> {
    await using client = await this.acquire();
    return client.query<T>(sql, params);
  }

  async queryOne<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined> {
    await using client = await this.acquire();
    return client.queryOne<T>(sql, params);
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
    await using client = await this.acquire();
    return client.queryArray<T>(sql, params);
  }

  async queryOneArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined> {
    await using client = await this.acquire();
    return client.queryOneArray<T>(sql, params);
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
    await using client = await this.acquire();

    return client.transaction(fn);
  }
}

export class PostgresPoolConnection
  extends CorePoolConnectionWrapper<PoolClient, PostgresTransaction> {
  release(): Promise<void> {
    this.client.release();
    return Promise.resolve();
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
}
