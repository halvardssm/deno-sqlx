/**
 * This file contain wrappers to wrap around existing
 * database clients until they have transitioned over
 * to using the shared standard interfaces.
 */

import {
  AbstractConnection,
  AbstractConnectionPool,
  ArrayRow,
  ConnectionOptions,
  ConnectionPoolOptions,
  CoreTransactionOptions,
  Param,
  PoolConnection,
  Row,
  TransactionQueriable,
} from "./interfaces.ts";

/**
 * Core connection wrapper
 *
 * Represents a single connection to a database.
 *
 * This is usually what you want to use for scripts.
 * If you are building a web application, you most likely want to use a connection pool (CoreConnectionPool) instead.
 */
export abstract class CoreConnectionWrapper<
  Client,
  ClientOptions extends ConnectionOptions,
  TransactionOptions extends CoreTransactionOptions,
  TransactionClient extends TransactionQueriable<TransactionOptions>,
> extends AbstractConnection<
  ClientOptions,
  TransactionOptions,
  TransactionClient
> {
  /**
   * Connection to the database
   */
  protected _client: Client | undefined;

  /**
   * Set the connection to the database
   */
  set client(connection: Client | undefined) {
    this._client = connection;
  }

  /**
   * Get the connection to the database
   */
  get client(): Client {
    if (!this._client) {
      throw new Error("Connection is not established, call connect() first.");
    }
    return this._client;
  }
}

/**
 * Core connection pool wrapper
 *
 * Represents a connection pool to a database.
 *
 * This is usually what you want to use when you are building a web application.
 * If you are writing simple scripts, you most likely want to use a single connection (CoreConnection) instead.
 */
export abstract class CoreConnectionPoolWrapper<
  Client,
  ClientOptions extends ConnectionPoolOptions,
  TransactionOptions extends CoreTransactionOptions,
  TransactionClient extends TransactionQueriable<TransactionOptions>,
  PoolClient extends PoolConnection<TransactionOptions, TransactionClient>,
> extends AbstractConnectionPool<
  ClientOptions,
  TransactionOptions,
  TransactionClient,
  PoolClient
> {
  /**
   * Connection to the database
   */
  protected _client: Client | undefined;

  /**
   * Set the connection to the database
   */
  set client(connection: Client | undefined) {
    this._client = connection;
  }

  /**
   * Get the connection to the database
   */
  get client(): Client {
    if (!this._client) {
      throw new Error("Connection is not established, call connect() first.");
    }
    return this._client;
  }
}

/**
 * Core pool connection wrapper
 *
 * Represents a single connection from a pool connection to a database.
 */
export abstract class CorePoolConnectionWrapper<
  Client,
  TransactionOptions extends CoreTransactionOptions,
  TransactionClient extends TransactionQueriable<TransactionOptions>,
> implements PoolConnection<TransactionOptions, TransactionClient> {
  /**
   * Connection to the database
   */
  readonly client: Client;

  /**
   * @param client The database client
   */
  constructor(client: Client) {
    this.client = client;
  }

  abstract release(): Promise<void>;
  abstract execute(sql: string, params?: Param[]): Promise<number | undefined>;
  abstract query<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T[]>;
  abstract queryOne<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined>;
  abstract queryMany<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<AsyncIterable<T>>;
  abstract queryArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<T[]>;
  abstract queryOneArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined>;
  abstract queryManyArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<AsyncIterable<T>>;
  abstract beginTransaction(
    options?: TransactionOptions["beginTransactionOptions"],
  ): Promise<TransactionClient>;
  abstract transaction<T>(
    fn: (connection: TransactionClient) => Promise<T>,
  ): Promise<T>;
  async [Symbol.asyncDispose](): Promise<void> {
    await this.release();
  }
}

/**
 * Core transaction client wrapper
 *
 * Represents a transaction client to a database.
 */
export abstract class CoreTransactionClientWrapper<
  TransactionClient,
  TransactionOptions extends CoreTransactionOptions,
> implements TransactionQueriable<TransactionOptions> {
  /**
   * Connection to the database
   */
  readonly client: TransactionClient;
  /**
   * @param connectionUrl Connection URL
   */
  constructor(transactionClient: TransactionClient) {
    this.client = transactionClient;
  }

  abstract execute(sql: string, params?: Param[]): Promise<number | undefined>;
  abstract query<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T[]>;
  abstract queryOne<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined>;
  abstract queryMany<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<AsyncIterable<T>>;
  abstract queryArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<T[]>;
  abstract queryOneArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined>;
  abstract queryManyArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<AsyncIterable<T>>;
  abstract commitTransaction(
    options?: TransactionOptions["commitTransactionOptions"],
  ): Promise<void>;
  abstract rollbackTransaction(
    options?: TransactionOptions["rollbackTransactionOptions"],
  ): Promise<void>;
  abstract createSavepoint(name: string): Promise<void>;
  abstract releaseSavepoint(name: string): Promise<void>;
}

export function emptyAsyncIterable<T>(): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]() {
      return {
        next: () => {
          return Promise.resolve({ done: true, value: undefined });
        },
      };
    },
  };
}

export function transformToAsyncIterable<
  T extends unknown,
  I extends IterableIterator<T>,
>(iterableIterator: I): Promise<AsyncIterable<T>> {
  return Promise.resolve({
    [Symbol.asyncIterator]: async function* () {
      for await (const item of iterableIterator) {
        yield item;
      }
    },
  });
}
