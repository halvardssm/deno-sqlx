import {
  ArrayRow,
  CoreTransactionClientWrapper,
  Param,
  Row,
} from "../core/mod.ts";
import { Transaction } from "@db/postgres";

export class PostgresTransaction
  extends CoreTransactionClientWrapper<Transaction> {
  async commit(savePoint?: string): Promise<void> {
    if (savePoint) {
      await this.client.savepoint(savePoint);
    } else {
      await this.client.commit();
    }
  }

  async rollback(savePoint?: string): Promise<void> {
    await this.client.rollback(savePoint);
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
}
