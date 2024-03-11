import {
  ArrayRow,
  CoreTransactionClientWrapper,
  emptyAsyncIterable,
  Param,
  Row,
} from "../core/mod.ts";
import { Connection } from "@db/mysql";

export class MySqlTransaction extends CoreTransactionClientWrapper<Connection> {
  async commit(savePoint?: string): Promise<void> {
    if (savePoint) {
      await this.client.execute("SAVEPOINT ?", [savePoint]);
    } else {
      await this.client.execute("COMMIT");
    }
  }

  async rollback(savePoint?: string): Promise<void> {
    if (savePoint) {
      await this.client.execute("ROLLBACK TO SAVEPOINT ?", [savePoint]);
    } else {
      await this.client.execute("ROLLBACK");
    }
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

  queryArray<T extends ArrayRow = ArrayRow>(
    _sql: string,
    _params?: Param[],
  ): Promise<T[]> {
    return Promise.reject(new Error("Method not implemented."));
  }

  queryOneArray<T extends ArrayRow = ArrayRow>(
    _sql: string,
    _params?: Param[],
  ): Promise<T | undefined> {
    return Promise.reject(new Error("Method not implemented."));
  }

  queryManyArray<T extends ArrayRow = ArrayRow>(
    _sql: string,
    _params?: Param[],
  ): Promise<AsyncIterable<T>> {
    return Promise.reject(new Error("Method not implemented."));
  }
}
