import {
  ArrayRow,
  CoreTransactionClientWrapper,
  CoreTransactionOptions,
  Row,
} from "../core/mod.ts";
import { Transaction, TransactionOptions } from "@db/postgres";
import { PostgresConnectionParamType } from "./connection.ts";

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

/**
 * Transaction wrapper for the PostgreSQL database.
 */
export class PostgresTransaction extends CoreTransactionClientWrapper<
  PostgresConnectionParamType,
  Transaction,
  PostgresTransactionOptions
> {
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
