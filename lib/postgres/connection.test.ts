import { describe, it } from "@std/testing/bdd";
import {
  assertEquals,
  assertInstanceOf,
  assertRejects,
  assertThrows,
} from "@std/assert";
import { PostgresConnection } from "./connection.ts";
import { Client } from "@db/postgres";

const dbUrl = "postgres://root:pwd@localhost:5100/sqlx";

describe("PostgresConnection", () => {
  it("should construct", async () => {
    await using db = new PostgresConnection(dbUrl);

    assertInstanceOf(db, PostgresConnection);
    assertEquals(db.connectionUrl, dbUrl);

    assertThrows(
      () => db.client,
      "Connection is not established, call connect() first.",
    );
  });

  it("should parse connection string (parseConnectionUrl)", () => {
    assertEquals(
      PostgresConnection.parseConnectionUrl(dbUrl),
      {
        hostname: "localhost",
        user: "root",
        password: "pwd",
        port: 5100,
        database: "sqlx",
        host_type: "tcp",
        options: {},
      },
    );

    assertEquals(
      PostgresConnection.parseConnectionUrl(
        "postgres://root:pwd@localhost:5100/sqlx?connect-timeout=30",
      ),
      {
        hostname: "localhost",
        user: "root",
        password: "pwd",
        port: 5100,
        database: "sqlx",
        host_type: "tcp",
        options: {
          "connect-timeout": "30",
        },
      },
    );
  });

  it("should connect and close", async () => {
    await using db = new PostgresConnection(dbUrl);

    await db.connect();

    assertInstanceOf(db.client, Client);

    await db.close();

    assertThrows(
      () => db.client,
      "Connection is not established, call connect() first.",
    );
  });

  it("should execute", async () => {
    await using db = new PostgresConnection(dbUrl);
    await db.connect();
    await db.execute("DROP TABLE IF EXISTS test");
    await db.execute(
      "CREATE TABLE test (name TEXT, age INTEGER)",
    );
    await db.execute("DROP TABLE test");
  });

  it("should query", async () => {
    await using db = new PostgresConnection(dbUrl);
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
    assertRejects(
      () => db.queryMany("SELECT * FROM test"),
      Error,
      "Method not implemented.",
    );
    const resQueryArray = await db.queryArray("SELECT * FROM test");
    assertEquals(resQueryArray, [
      ["John", 42],
      ["Jane", 36],
    ]);
    const resQueryOneArray = await db.queryOneArray("SELECT * FROM test");
    assertEquals(resQueryOneArray, ["John", 42]);
    assertRejects(
      () => db.queryManyArray("SELECT * FROM test"),
      Error,
      "Method not implemented.",
    );
    await db.execute("DROP TABLE test");
  });

  it("should transaction", async () => {
    await using db = new PostgresConnection(dbUrl);
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
