import {
  ArrayRow,
  CoreTransactionClientWrapper,
  CoreTransactionOptions,
  emptyAsyncIterable,
  Row,
} from "../core/mod.ts";
import { Connection } from "@db/mysql";
import { MySqlConnectionParamType } from "./connection.ts";

export interface MySqlTransactionOptions extends CoreTransactionOptions {
  beginTransactionOptions: {
    withConsistentSnapshot?: boolean;
    readWrite?: "READ WRITE" | "READ ONLY";
  };
  commitTransactionOptions: {
    chain?: boolean;
    release?: boolean;
  };
  rollbackTransactionOptions: {
    chain?: boolean;
    release?: boolean;
    toSavepoint?: string;
  };
}

/**
 * Transaction wrapper for the MySQL database.
 */
export class MySqlTransaction extends CoreTransactionClientWrapper<
  MySqlConnectionParamType,
  Connection,
  MySqlTransactionOptions
> {
  /**
   * @inheritdoc
   */
  async commitTransaction(
    options?: MySqlTransactionOptions["commitTransactionOptions"],
  ): Promise<void> {
    let query = "COMMIT";

    if (options?.chain === true) {
      query += " AND CHAIN";
    } else if (options?.chain === false) {
      query += " AND NO CHAIN";
    }

    if (options?.release === true) {
      query += " RELEASE";
    } else if (options?.release === false) {
      query += " NO RELEASE";
    }

    await this.client.execute(query);
  }

  /**
   * @inheritdoc
   */
  async rollbackTransaction(
    options?: MySqlTransactionOptions["rollbackTransactionOptions"],
  ): Promise<void> {
    let query = "ROLLBACK";

    if (options?.toSavepoint) {
      query += " TO ?";
      await this.client.execute(query, [options.toSavepoint]);
      return;
    }

    if (options?.chain === true) {
      query += " AND CHAIN";
    } else if (options?.chain === false) {
      query += " AND NO CHAIN";
    }

    if (options?.release === true) {
      query += " RELEASE";
    } else if (options?.release === false) {
      query += " NO RELEASE";
    }

    await this.client.execute(query);
  }

  /**
   * @inheritdoc
   */
  async createSavepoint(name: string): Promise<void> {
    await this.client.execute("SAVEPOINT ?", [name]);
  }

  /**
   * @inheritdoc
   */
  async releaseSavepoint(name: string): Promise<void> {
    await this.client.execute("RELEASE SAVEPOINT ?", [name]);
  }

  /**
   * @inheritdoc
   */
  async execute(
    sql: string,
    params?: MySqlConnectionParamType[],
  ): Promise<number | undefined> {
    const res = await this.client.execute(sql, params);
    return res.affectedRows;
  }

  /**
   * @inheritdoc
   */
  async query<
    T extends Row<MySqlConnectionParamType> = Row<MySqlConnectionParamType>,
  >(
    sql: string,
    params?: MySqlConnectionParamType[],
  ): Promise<T[]> {
    const res = await this.client.execute(sql, params);
    return (res.rows || []) as T[];
  }

  /**
   * @inheritdoc
   */
  async queryOne<
    T extends Row<MySqlConnectionParamType> = Row<MySqlConnectionParamType>,
  >(
    sql: string,
    params?: MySqlConnectionParamType[],
  ): Promise<T | undefined> {
    const res = await this.client.execute(sql, params);
    return ((res.rows || []) as T[])[0];
  }

  /**
   * @inheritdoc
   */
  async queryMany<
    T extends Row<MySqlConnectionParamType> = Row<MySqlConnectionParamType>,
  >(
    sql: string,
    params?: MySqlConnectionParamType[],
  ): Promise<AsyncIterable<T>> {
    const res = await this.client.execute(sql, params, true);
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
}
