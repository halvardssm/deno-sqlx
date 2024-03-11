# Deno SQLx

Standard interfaces for SQL like databases for Deno or general JS.

Inspired by [rust sqlx](https://docs.rs/sqlx/latest/sqlx/index.html) and
[go sql](https://pkg.go.dev/database/sql).

The goal for this repo is to have a standard interface for SQL like database
clients that can be used in Deno, Node and other JS runtimes.

A subgoal is for this repo to live in Deno Standard Library and available in
[JSR](https://jsr.io/) under `@std/database` (or `@std/sql` or similar), and/or
be available under `@db/core`.

This repo is WIP and can change at any time.

## Usage

### New database clients

For new database clients, you want to expose at the minimum a class extending
`AbstractConnection` with all methods defined.

If your client also supports a client pool, you want to expose a class extending
`AbstractConnectionPool` with all methods defined.

### Existing database clients

For existing database clients, there are two ways of approach:

1. Rewrite your database clients following the
   [new database client section](#new-database-clients).
2. Create new classes inheriting the `CoreConnectionPoolWrapper` and
   `CoreConnectionPoolWrapper` (and optionally `CorePoolConnectionWrapper` and
   `CoreTransactionClientWrapper` if needed), and expose these alongside your
   existing clients. This should only be as a temporary step while taking the
   time to rewrite your existing clients as this enables support for the
   standard interface faster.

## Usage

### New database clients

For new database clients, you want to expose at the minimum a class extending
`AbstractConnection` with all methods defined.

If your client also supports a client pool, you want to expose a class extending
`AbstractConnectionPool` with all methods defined.

### Existing database clients

For existing database clients, there are two ways of approach:

1. Rewrite your database clients following the
   [new database client section](#new-database-clients).
2. Create new classes inheriting the `CoreConnectionPoolWrapper` and
   `CoreConnectionPoolWrapper` (and optionally `CorePoolConnectionWrapper` and
   `CoreTransactionClientWrapper` if needed), and expose these alongside your
   existing clients. This should only be as a temporary step while taking the
   time to rewrite your existing clients as this enables support for the
   standard interface faster.
