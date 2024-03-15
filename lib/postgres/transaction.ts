import {
  ArrayRow,
  CoreTransactionClientWrapper,
  CoreTransactionOptions,
  Param,
  Row,
} from "../core/mod.ts";
import { Transaction, TransactionOptions } from "@db/postgres";

export interface PostgresTransactionOptions extends CoreTransactionOptions {
  beginTransactionOptions: TransactionOptions & {
    name: string;
  };
  commitTransactionOptions: {
    chain?: boolean;
  };
  rollbackTransactionOptions: {
    chain?: boolean;
    savepoint?: string;
  };
}

export class PostgresTransaction extends CoreTransactionClientWrapper<
  Transaction,
  PostgresTransactionOptions
> {
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

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
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

  async commitTransaction(
    options?: PostgresTransactionOptions["commitTransactionOptions"],
  ): Promise<void> {
    await this.client.commit(options);
  }

  async rollbackTransaction(
    options?: PostgresTransactionOptions["rollbackTransactionOptions"],
  ): Promise<void> {
    await this.client.rollback(options);
  }

  async createSavepoint(name: string): Promise<void> {
    await this.client.savepoint(name);
  }

  async releaseSavepoint(name: string): Promise<void> {
    await this.client.getSavepoint(name)?.release();
  }
}
