import { describe, it } from "@std/testing/bdd";
import {
  assert,
  assertEquals,
  assertInstanceOf,
  assertThrows,
} from "@std/assert";
import { Pool } from "@db/postgres";
import { PostgresConnectionPool, PostgresPoolConnection } from "./pool.ts";

const dbUrl = "postgres://root:pwd@localhost:5100/sqlx";

describe("PostgresConnectionPool", () => {
  it("should construct", async () => {
    await using db = new PostgresConnectionPool(dbUrl, { poolSize: 3 });

    assertInstanceOf(db, PostgresConnectionPool);
    assertEquals(db.connectionUrl, dbUrl);

    assertThrows(
      () => db.client,
      "Connection is not established, call connect() first.",
    );
  });

  it("should connect and close", async () => {
    await using db = new PostgresConnectionPool(dbUrl, { poolSize: 3 });

    await db.connect();

    assertInstanceOf(db.client, Pool);

    await db.close();

    assertThrows(
      () => db.client,
      "Connection is not established, call connect() first.",
    );
  });

  it("should aquire and release", async () => {
    await using db = new PostgresConnectionPool(dbUrl, { poolSize: 3 });

    await db.connect();
    assertInstanceOf(db.client, Pool);

    await using p1 = await db.acquire();
    assertInstanceOf(p1, PostgresPoolConnection);
    await using p2 = await db.acquire();
    assertInstanceOf(p2, PostgresPoolConnection);
    await using p3 = await db.acquire();
    assertInstanceOf(p3, PostgresPoolConnection);

    await p1.execute("DROP TABLE IF EXISTS test");
    await p1.execute(
      "CREATE TABLE test (name TEXT, age INTEGER)",
    );
    await p2.execute("INSERT INTO test (name, age) VALUES ($1, $2)", [
      "John",
      42,
    ]);
    await p3.execute("INSERT INTO test (name, age) VALUES ($1, $2)", [
      "Jane",
      36,
    ]);
    p1.release();

    const resQuery = await db.query("SELECT * FROM test");
    assertEquals(resQuery, [
      { name: "John", age: 42 },
      { name: "Jane", age: 36 },
    ]);
  });

  it("should execute", async () => {
    await using db = new PostgresConnectionPool(dbUrl, { poolSize: 3 });
    await db.connect();
    await db.execute("DROP TABLE IF EXISTS test");
    await db.execute(
      "CREATE TABLE test (name TEXT, age INTEGER)",
    );
    await db.execute("DROP TABLE test");
  });

  it("should query", async () => {
    await using db = new PostgresConnectionPool(dbUrl, { poolSize: 3 });
    await db.connect();
    await db.execute("DROP TABLE IF EXISTS test");
    await db.execute(
      "CREATE TABLE test (name TEXT, age INTEGER)",
    );
    await db.execute("INSERT INTO test (name, age) VALUES ($1, $2)", [
      "John",
      42,
    ]);
    await db.execute("INSERT INTO test (name, age) VALUES ($1, $2)", [
      "Jane",
      36,
    ]);
    const resQuery = await db.query("SELECT * FROM test");
    assertEquals(resQuery, [
      { name: "John", age: 42 },
      { name: "Jane", age: 36 },
    ]);
    const resQueryOne = await db.queryOne("SELECT * FROM test");
    assertEquals(resQueryOne, { name: "John", age: 42 });
    try {
      await db.queryMany("SELECT * FROM test");
      assert(false, "Should throw error");
    } catch (e) {
      assertInstanceOf(e, Error);
      assertEquals(e.message, "Method not implemented.");
    }
    const resQueryArray = await db.queryArray("SELECT * FROM test");
    assertEquals(resQueryArray, [
      ["John", 42],
      ["Jane", 36],
    ]);
    const resQueryOneArray = await db.queryOneArray("SELECT * FROM test");
    assertEquals(resQueryOneArray, ["John", 42]);
    try {
      await db.queryManyArray("SELECT * FROM test");
      assert(false, "Should throw error");
    } catch (e) {
      assertInstanceOf(e, Error);
      assertEquals(e.message, "Method not implemented.");
    }
    await db.execute("DROP TABLE test");
  });

  it("should transaction", async () => {
    await using db = new PostgresConnectionPool(dbUrl, { poolSize: 3 });
    await db.connect();
    await db.execute("DROP TABLE IF EXISTS test");
    await db.execute(
      "CREATE TABLE test (name TEXT, age INTEGER)",
    );
    const res: { name: string; age: number }[] = [];
    const res2 = await db.transaction(async (conn) => {
      await conn.execute("INSERT INTO test (name, age) VALUES ($1, $2)", [
        "John",
        42,
      ]);
      const resQuery = await conn.query<{ name: string; age: number }>(
        "SELECT * FROM test",
      );
      for await (const row of resQuery) {
        res.push(row);
      }
      await conn.execute("INSERT INTO test (name, age) VALUES ($1, $2)", [
        "Jane",
        36,
      ]);
      const resQuery2 = await conn.query<{ name: string; age: number }>(
        "SELECT * FROM test",
      );
      for await (const row of resQuery2) {
        res.push(row);
      }
      return res;
    });

    assertEquals(res, [
      { name: "John", age: 42 },
      { name: "John", age: 42 },
      { name: "Jane", age: 36 },
    ]);
    assertEquals(res2, [
      { name: "John", age: 42 },
      { name: "John", age: 42 },
      { name: "Jane", age: 36 },
    ]);
  });
});
