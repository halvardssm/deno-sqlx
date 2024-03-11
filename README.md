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
