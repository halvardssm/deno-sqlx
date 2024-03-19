// deno-lint-ignore-file require-await
import {
  ArrayRow,
  CoreTransactionClientWrapper,
  CoreTransactionOptions,
  Row,
  transformToAsyncIterable,
} from "../core/mod.ts";
import { Database } from "@db/sqlite";
import { SqLiteConnectionParamType } from "./connection.ts";

export interface SqLiteTransactionOptions extends CoreTransactionOptions {
  beginTransactionOptions: {
    behavior?: "DEFERRED" | "IMMEDIATE" | "EXCLUSIVE";
  };
  commitTransactionOptions: undefined;
  rollbackTransactionOptions: {
    savepoint?: string;
  };
}

/**
 * Transaction wrapper for the SQLite database.
 */
export class SqLiteTransaction extends CoreTransactionClientWrapper<
  SqLiteConnectionParamType,
  Database,
  SqLiteTransactionOptions
> {
  async execute(
    sql: string,
    params?: SqLiteConnectionParamType[],
  ): Promise<number | undefined> {
    return this.client.exec(sql, ...(params || []));
  }

  async query<
    T extends Row<SqLiteConnectionParamType> = Row<SqLiteConnectionParamType>,
  >(
    sql: string,
    params?: SqLiteConnectionParamType[],
  ): Promise<T[]> {
    const res = this.client.prepare(sql).all<T>(...(params || []));
    return res;
  }

  async queryOne<
    T extends Row<SqLiteConnectionParamType> = Row<SqLiteConnectionParamType>,
  >(
    sql: string,
    params?: SqLiteConnectionParamType[],
  ): Promise<T | undefined> {
    const res = this.client.prepare(sql).get<T>(...(params || []));
    return res;
  }

  async queryMany<
    T extends Row<SqLiteConnectionParamType> = Row<SqLiteConnectionParamType>,
  >(
    sql: string,
    params?: SqLiteConnectionParamType[] | undefined,
  ): Promise<AsyncIterable<T>> {
    return transformToAsyncIterable(
      this.client.prepare(sql).bind(...(params || []))[Symbol.iterator](),
    );
  }
  async queryArray<
    T extends ArrayRow<SqLiteConnectionParamType> = ArrayRow<
      SqLiteConnectionParamType
    >,
  >(
    sql: string,
    params?: SqLiteConnectionParamType[] | undefined,
  ): Promise<T[]> {
    const res = this.client.prepare(sql).values<T>(...(params || []));
    return res;
  }
  async queryOneArray<
    T extends ArrayRow<SqLiteConnectionParamType> = ArrayRow<
      SqLiteConnectionParamType
    >,
  >(
    sql: string,
    params?: SqLiteConnectionParamType[] | undefined,
  ): Promise<T | undefined> {
    const res = this.client.prepare(sql).value<T>(...(params || []));
    return res;
  }

  /**
   * Method not implemented.
   *
   * @throws {Error} Method not implemented.
   */
  async queryManyArray<
    T extends ArrayRow<SqLiteConnectionParamType> = ArrayRow<
      SqLiteConnectionParamType
    >,
  >(
    _sql: string,
    _params?: SqLiteConnectionParamType[] | undefined,
  ): Promise<AsyncIterable<T>> {
    return Promise.reject(new Error("Method not implemented."));
  }

  async createSavepoint(name: string): Promise<void> {
    this.client.exec("SAVEPOINT ?", name);
  }

  async releaseSavepoint(name: string): Promise<void> {
    this.client.exec("RELEASE ?", name);
  }

  async commitTransaction(): Promise<void> {
    this.client.exec("COMMIT");
  }

  async rollbackTransaction(
    options?: SqLiteTransactionOptions["rollbackTransactionOptions"],
  ): Promise<void> {
    if (options?.savepoint) {
      this.client.exec("ROLLBACK TO ?", options.savepoint);
    } else {
      this.client.exec("ROLLBACK");
    }
  }
}
