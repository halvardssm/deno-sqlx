/**
 * Helpers
 */

/**
 * Parameter type for SQL queries.
 * This is a union of all types that can be used as parameters in SQL queries.
 */
export type Param = string | number | boolean | null;

/**
 * Row type for SQL queries, represented as an object entry.
 */
export type Row = Record<string, unknown>;

/**
 * Row type for SQL queries, represented as an array entry.
 */
export type ArrayRow = unknown[];

/**
 * Interfaces for collected database features
 */

/**
 * Connectable interface
 *
 * Represents an object that can connect and disconnect from a database.
 */
export interface Connectable<Options extends ConnectionOptions> {
  /**
   * Connection URL
   */
  readonly connectionUrl: string;

  /**
   * Connection options
   */
  readonly connectionOptions: Options;

  /**
   * Create a connection to the database
   */
  connect(): Promise<void>;
  /**
   * Close the connection to the database
   */
  close(): Promise<void>;

  /**
   * Disposes the connection to the database on exit when using the `using` keyword
   *
   * @example
   * ```ts
   * await using connection = new Connection()
   * ```
   */
  [Symbol.asyncDispose](): Promise<void>;
}

/**
 * Queriable interface
 *
 * Represents an object that can execute SQL queries.
 */
export interface Queriable {
  /**
   * Execute a SQL statement
   *
   * @param sql the SQL statement
   * @param params the parameters to bind to the SQL statement
   * @returns the number of affected rows if any
   */
  execute(sql: string, params?: Param[]): Promise<number | undefined>;
  /**
   * Query the database
   *
   * @param sql the SQL statement
   * @param params the parameters to bind to the SQL statement
   * @returns the rows returned by the query as object entries
   */
  query<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T[]>;
  /**
   * Query the database and return at most one row
   *
   * @param sql the SQL statement
   * @param params the parameters to bind to the SQL statement
   * @returns the row returned by the query as an object entry, or undefined if no row is returned
   */
  queryOne<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined>;
  /**
   * Query the database and return an iterator.
   * Usefull when querying large datasets, as this should take advantage of data streams.
   *
   * @param sql the SQL statement
   * @param params the parameters to bind to the SQL statement
   * @returns the rows returned by the query as object entries
   */
  queryMany<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<AsyncIterable<T>>;
  /**
   * Query the database
   *
   * @param sql the SQL statement
   * @param params the parameters to bind to the SQL statement
   * @returns the rows returned by the query as array entries
   */
  queryArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<T[]>;
  /**
   * Query the database and return at most one row
   *
   * @param sql the SQL statement
   * @param params the parameters to bind to the SQL statement
   * @returns the row returned by the query as an array entry, or undefined if no row is returned
   */
  queryOneArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined>;

  /**
   * Query the database and return an iterator.
   * Usefull when querying large datasets, as this should take advantage of data streams.
   *
   * @param sql the SQL statement
   * @param params the parameters to bind to the SQL statement
   * @returns the rows returned by the query as array entries
   */
  queryManyArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[],
  ): Promise<AsyncIterable<T>>;
}

/**
 * Transactionable interface
 *
 * Represents an object that can execute a transaction.
 */
export interface Transactionable<
  TransactionClient extends TransactionQueriable,
> {
  /**
   * Create a transaction
   *
   * If the callback function throws an error, the transaction will be rolled back and the error will be rethrown.
   * If the callback function returns successfully, the transaction will be committed.
   *
   * @param fn callback function to be executed within a transaction
   * @returns the result of the callback function
   */
  transaction<T>(
    fn: (connection: TransactionClient) => Promise<T>,
  ): Promise<T>;
}

/**
 * Poolable interface
 *
 * Represents an object that can acquire a connection from a pool.
 */
export interface Poolable<Connection> {
  poolSize: number;
  /**
   * Acquire a connection from the pool
   */
  acquire(): Promise<Connection>;
}

/**
 * TransactionQueriable interface
 *
 * Represents a transaction client to a database.
 */
export interface TransactionQueriable extends Queriable {
  /**
   * Commit the transaction
   *
   * @param savePoint commits to a save point if provided, otherwise commits and ends the transaction
   */
  commit(savePoint?: string): Promise<void>;
  /**
   * Rollback the transaction
   *
   * @param savePoint rollbacks to a save point if provided, otherwise rollbacks and ends the transaction
   */
  rollback(savePoint?: string): Promise<void>;
}

/**
 * Database interfaces that should be implemented by database
 * drivers and exposed to the user
 */

export interface ConnectionOptions extends Record<string, unknown> {}

/**
 * Connection interface
 *
 * This represents a connection to a database.
 * When a user wants a single connection to the database,
 * they should use a class implementing this interface.
 */
export interface Connection<
  Options extends ConnectionOptions,
  TransactionClient extends TransactionQueriable,
> extends Connectable<Options>, Queriable, Transactionable<TransactionClient> {
}

/**
 * ConnectionPool interface
 *
 * This represents a pool of connections to a database.
 * When a user wants to use a pool of connections to the database,
 * they should use a class implementing this interface.
 */
export interface ConnectionPool<
  Options extends ConnectionPoolOptions,
  TransactionClient extends TransactionQueriable,
  Pool extends PoolConnection<TransactionClient>,
> extends Connection<Options, TransactionClient>, Poolable<Pool> {
}

export interface ConnectionPoolOptions extends ConnectionOptions {
  /**
   * Maximum number of connections in the pool
   */
  poolSize: number;
}

/**
 * PoolConnection interface
 *
 * This represents a connection to a database from a pool.
 * When a user wants to use a connection from a pool,
 * they should use a class implementing this interface.
 */
export interface PoolConnection<TransactionClient extends TransactionQueriable>
  extends Queriable, Transactionable<TransactionClient> {
  /**
   * Release the connection to the pool
   */
  release(): Promise<void>;

  /**
   * Releases the connection to the pool on exit when using the `using` keyword
   *
   * @example
   * ```ts
   * using connection = await new Connection().connect();
   * ```
   */
  [Symbol.asyncDispose](): Promise<void>;
}

/**
 * Abstract classes that should be implemented by database
 */

/**
 * AbstractConnection class
 *
 * This class should be extended by database drivers to implement the Connection interface.
 */
export abstract class AbstractConnection<
  Options extends ConnectionOptions,
  TransactionClient extends TransactionQueriable,
> implements Connection<Options, TransactionClient> {
  connectionUrl: string;
  connectionOptions: Options;

  constructor(connectionUrl: string, connectionOptions: Options) {
    this.connectionUrl = connectionUrl;
    this.connectionOptions = connectionOptions;
  }

  abstract connect(): Promise<void>;
  abstract close(): Promise<void>;
  abstract execute(
    sql: string,
    params?: Param[] | undefined,
  ): Promise<number | undefined>;
  abstract query<T extends Row = Row>(
    sql: string,
    params?: Param[] | undefined,
  ): Promise<T[]>;
  abstract queryOne<T extends Row = Row>(
    sql: string,
    params?: Param[] | undefined,
  ): Promise<T | undefined>;
  abstract queryMany<T extends Row = Row>(
    sql: string,
    params?: Param[] | undefined,
  ): Promise<AsyncIterable<T>>;
  abstract queryArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[] | undefined,
  ): Promise<T[]>;
  abstract queryOneArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[] | undefined,
  ): Promise<T | undefined>;
  abstract queryManyArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[] | undefined,
  ): Promise<AsyncIterable<T>>;
  abstract transaction<T>(
    fn: (connection: TransactionClient) => Promise<T>,
  ): Promise<T>;
  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}

/**
 * AbstractConnectionPool class
 *
 * This class should be extended by database drivers to implement the ConnectionPool interface.
 */
export abstract class AbstractConnectionPool<
  Options extends ConnectionPoolOptions,
  TransactionClient extends TransactionQueriable,
  Pool extends PoolConnection<TransactionClient>,
> extends AbstractConnection<Options, TransactionClient>
  implements ConnectionPool<Options, TransactionClient, Pool> {
  poolSize: number;

  constructor(connectionUrl: string, connectionOptions: Options) {
    super(connectionUrl, connectionOptions);
    this.poolSize = connectionOptions.poolSize;
  }

  abstract acquire(): Promise<Pool>;
}
