import { describe, it } from "@std/testing/bdd";
import {
  assertEquals,
  assertInstanceOf,
  assertRejects,
  assertThrows,
} from "@std/assert";
import { MySqlConnection } from "./connection.ts";
import { Client } from "@db/mysql";

const dbUrl = "mysql://root@0.0.0.0:5101/sqlx";

describe("MySqlConnection", () => {
  it("should construct", async () => {
    await using db = new MySqlConnection(dbUrl);

    assertInstanceOf(db, MySqlConnection);
    assertEquals(db.connectionUrl, dbUrl);

    assertThrows(
      () => db.client,
      "Connection is not established, call connect() first.",
    );
  });

  it("should parse connection string (parseConnectionUrl)", () => {
    assertEquals(
      MySqlConnection.parseConnectionUrl(dbUrl),
      {
        hostname: "0.0.0.0",
        username: "root",
        password: undefined,
        port: 5101,
        db: "sqlx",
      },
    );

    assertEquals(
      MySqlConnection.parseConnectionUrl(
        "mysql://root:pwd@localhost:5101/sqlx?connect-timeout=30&ssl-mode=verify_identity&ssl-ca=.test/testcert.pem&ssl-capath=.test",
      ),
      {
        hostname: "localhost",
        username: "root",
        password: "pwd",
        port: 5101,
        db: "sqlx",
      },
    );
  });

  it("should connect and close", async () => {
    await using db = new MySqlConnection(dbUrl);

    await db.connect();

    assertInstanceOf(db.client, Client);

    await db.close();

    assertThrows(
      () => db.client,
      "Connection is not established, call connect() first.",
    );
  });

  it("should execute", async () => {
    await using db = new MySqlConnection(dbUrl);
    await db.connect();
    await db.execute("DROP TABLE IF EXISTS test");
    await db.execute(
      "CREATE TABLE test (name TEXT, age INTEGER)",
    );
    await db.execute("DROP TABLE test");
  });

  it("should query", async () => {
    await using db = new MySqlConnection(dbUrl);
    await db.connect();
    await db.execute("DROP TABLE IF EXISTS test");
    await db.execute(
      "CREATE TABLE test (name TEXT, age INTEGER)",
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
      { name: "John", age: 42 },
      { name: "Jane", age: 36 },
    ]);
    const resQueryOne = await db.queryOne("SELECT * FROM test");
    assertEquals(resQueryOne, { name: "John", age: 42 });
    const resQueryMany = await db.queryMany("SELECT * FROM test");
    const resQueryManyRes = [];
    for await (const row of resQueryMany) {
      resQueryManyRes.push(row);
    }
    assertEquals(resQueryManyRes, [
      { name: "John", age: 42 },
      { name: "Jane", age: 36 },
    ]);
    assertRejects(
      () => db.queryArray("SELECT * FROM test"),
      Error,
      "Method not implemented.",
    );
    assertRejects(
      () => db.queryOneArray("SELECT * FROM test"),
      Error,
      "Method not implemented.",
    );
    assertRejects(
      () => db.queryManyArray("SELECT * FROM test"),
      Error,
      "Method not implemented.",
    );
    await db.execute("DROP TABLE test");
  });

  it("should transaction", async () => {
    await using db = new MySqlConnection(dbUrl);
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
