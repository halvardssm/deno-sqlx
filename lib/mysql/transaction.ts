import {
  ArrayRow,
  CoreTransactionClientWrapper,
  CoreTransactionOptions,
  emptyAsyncIterable,
  Param,
  Row,
} from "../core/mod.ts";
import { Connection } from "@db/mysql";

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

export class MySqlTransaction
  extends CoreTransactionClientWrapper<Connection, MySqlTransactionOptions> {
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

  async createSavepoint(name: string): Promise<void> {
    await this.client.execute("SAVEPOINT ?", [name]);
  }
  async releaseSavepoint(name: string): Promise<void> {
    await this.client.execute("RELEASE SAVEPOINT ?", [name]);
  }

  async execute(sql: string, params?: Param[]): Promise<number | undefined> {
    const res = await this.client.execute(sql, params);
    return res.affectedRows;
  }

  async query<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T[]> {
    const res = await this.client.execute(sql, params);
    return (res.rows || []) as T[];
  }

  async queryOne<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined> {
    const res = await this.client.execute(sql, params);
    return ((res.rows || []) as T[])[0];
  }

  async queryMany<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<AsyncIterable<T>> {
    const res = await this.client.execute(sql, params, true);
    return (res.iterator ?? emptyAsyncIterable()) as AsyncIterable<T>;
  }

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
  queryArray<T extends ArrayRow = ArrayRow>(
    _sql: string,
    _params?: Param[],
  ): Promise<T[]> {
    return Promise.reject(new Error("Method not implemented."));
  }

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
  queryOneArray<T extends ArrayRow = ArrayRow>(
    _sql: string,
    _params?: Param[],
  ): Promise<T | undefined> {
    return Promise.reject(new Error("Method not implemented."));
  }

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
  queryManyArray<T extends ArrayRow = ArrayRow>(
    _sql: string,
    _params?: Param[],
  ): Promise<AsyncIterable<T>> {
    return Promise.reject(new Error("Method not implemented."));
  }
}
