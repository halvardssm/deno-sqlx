import { describe, it } from "@std/testing/bdd";
import {
  assertEquals,
  assertInstanceOf,
  assertRejects,
  assertThrows,
} from "@std/assert";
import { SqLiteConnection } from "./connection.ts";
import { Database } from "@db/sqlite";

describe("SqLiteConnection", () => {
  it("should construct", async () => {
    await using db = new SqLiteConnection(":memory:");

    assertInstanceOf(db, SqLiteConnection);
    assertEquals(db.connectionUrl, ":memory:");

    assertThrows(
      () => db.client,
      "Connection is not established, call connect() first.",
    );
  });

  it("should connect and close", async () => {
    await using db = new SqLiteConnection(":memory:");

    await db.connect();

    assertInstanceOf(db.client, Database);

    await db.close();

    assertThrows(
      () => db.client,
      "Connection is not established, call connect() first.",
    );
  });

  it("should execute", async () => {
    await using db = new SqLiteConnection(":memory:");
    await db.connect();
    await db.execute(
      "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
  });

  it("should query", async () => {
    await using db = new SqLiteConnection(":memory:");
    await db.connect();
    await db.execute(
      "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    );
    await db.execute("INSERT INTO test (name, age) VALUES (?, ?)", [
      "John",
      42,
    ]);
    await db.execute("INSERT INTO test (name, age) VALUES (?, ?)", [
      "Jane",
      36,
    ]);
    const resQuery = await db.query("SELECT * FROM test");
    assertEquals(resQuery, [
      { id: 1, name: "John", age: 42 },
      { id: 2, name: "Jane", age: 36 },
    ]);
    const resQueryOne = await db.queryOne("SELECT * FROM test WHERE id = ?", [
      2,
    ]);
    assertEquals(resQueryOne, { id: 2, name: "Jane", age: 36 });
    const resQueryMany = await db.queryMany("SELECT * FROM test");
    const resQueryManyRes = [];
    for await (const row of resQueryMany) {
      resQueryManyRes.push(row);
    }
    assertEquals(resQueryManyRes, [
      { id: 1, name: "John", age: 42 },
      { id: 2, name: "Jane", age: 36 },
    ]);
    const resQueryArray = await db.queryArray("SELECT * FROM test");
    assertEquals(resQueryArray, [
      [1, "John", 42],
      [2, "Jane", 36],
    ]);
    const resQueryOneArray = await db.queryOneArray(
      "SELECT * FROM test WHERE id = ?",
      [2],
    );
    assertEquals(resQueryOneArray, [2, "Jane", 36]);
    assertRejects(
      () => db.queryManyArray("SELECT * FROM test"),
      Error,
      "Method not implemented.",
    );
  });

  it("should transaction", async () => {
    await using db = new SqLiteConnection(":memory:");
    await db.connect();
    await db.execute("DROP TABLE IF EXISTS test");
    await db.execute(
      "CREATE TABLE test (name TEXT, age INTEGER)",
    );
    const res: { name: string; age: number }[] = [];
    const res2 = await db.transaction(async (conn) => {
      await conn.execute("INSERT INTO test (name, age) VALUES (?, ?)", [
        "John",
        42,
      ]);
      const resQuery = await conn.query<{ name: string; age: number }>(
        "SELECT * FROM test",
      );
      for await (const row of resQuery) {
        res.push(row);
      }
      await conn.execute("INSERT INTO test (name, age) VALUES (?, ?)", [
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
