// deno-lint-ignore-file require-await
import {
  ArrayRow,
  CoreTransactionClientWrapper,
  Param,
  Row,
  transformToAsyncIterable,
} from "../core/mod.ts";
import { Database } from "@db/sqlite";

export class SqLiteTransactionClient
  extends CoreTransactionClientWrapper<Database> {
  async commit(): Promise<void> {
    this.client.exec("COMMIT");
  }

  async rollback(): Promise<void> {
    this.client.exec("ROLLBACK");
  }

  async execute(sql: string, params?: Param[]): Promise<number | undefined> {
    return this.client.exec(sql, ...(params || []));
  }

  async query<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T[]> {
    const res = this.client.prepare(sql).all<T>(...(params || []));
    return res;
  }

  async queryOne<T extends Row = Row>(
    sql: string,
    params?: Param[],
  ): Promise<T | undefined> {
    const res = this.client.prepare(sql).get<T>(...(params || []));
    return res;
  }

  async queryMany<T extends Row = Row>(
    sql: string,
    params?: Param[] | undefined,
  ): Promise<AsyncIterable<T>> {
    return transformToAsyncIterable(
      this.client.prepare(sql).bind(...(params || []))[Symbol.iterator](),
    );
  }
  async queryArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[] | undefined,
  ): Promise<T[]> {
    const res = this.client.prepare(sql).values<T>(...(params || []));
    return res;
  }
  async queryOneArray<T extends ArrayRow = ArrayRow>(
    sql: string,
    params?: Param[] | undefined,
  ): Promise<T | undefined> {
    const res = this.client.prepare(sql).value<T>(...(params || []));
    return res;
  }
  async queryManyArray<T extends ArrayRow = ArrayRow>(
    _sql: string,
    _params?: Param[] | undefined,
  ): Promise<AsyncIterable<T>> {
    return Promise.reject(new Error("Method not implemented."));
  }
}
