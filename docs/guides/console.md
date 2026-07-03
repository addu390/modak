# Console

`modak-console.jar` is a strict superset of the worker binary: the same daemon
and CLI, plus an embedded web console served on `MODAK_CONSOLE_PORT` (default
9090). The local stack runs it at [http://localhost:9090](http://localhost:9090).
Zero build step, zero external services: plain HTML/CSS/JS with Apache ECharts
and CodeMirror bundled as WebJars, served from the jar.

## Overview

The fleet at a glance: per-table mode, cut-line, mirror lag, delta backlog,
read pins, partition states, and in-flight initial copies, refreshed live.

Charts for mirror lag, delta backlog, slot WAL, and lake commit rate draw
from an in-process ring buffer (about 11 hours of history at the default sweep
interval), so no Prometheus is required for a useful picture.

## Table detail

Drill into any table: its partitions and lifecycle states, the operation
journal (tiering/compaction/maintenance phases), and replication slot detail
for mirrored tables.

## SQL playground

A schema browser, a CodeMirror SQL editor with snippets and history, and a
results grid. Statements run with transparent reads on, so tiered tables read
as your users see them, merged across both tiers, with a query timeout and a
row cap applied. Use it to create tables, inspect `modak.*`, or sanity-check
what a two-tier read returns.

The Explain button runs
[`modak_explain`](../reference/sql.md#modak_explainsql-text-setof-text) on the
statement instead of executing it, and shows where rows will come from or go
to: which tiers a read spans, whether a write passes through or splits into
hot and cold halves, and what would be rejected.

!!! warning "The playground is a superuser surface"
    Statements execute with the worker's Postgres credentials. Set
    `MODAK_CONSOLE_SQL=false` to disable the query endpoint, or deploy the
    headless `modak-worker.jar`, which has no console at all. Either way, keep
    the port internal: the console has no TLS and no auth by design.

## JSON API

Everything the UI shows is fetchable directly:

| Endpoint | Returns |
|----------|---------|
| `/api/overview` | Fleet summary: tables, cutlines, backlogs, slots, copies |
| `/api/table?id=<table_id>` | One table: partitions, journal, slot detail |
| `/api/series` | The chart ring buffers, `key -> [[ts, value], ...]` |
| `/api/schema` | Schema tree for the playground browser |
| `/api/query` | `POST` a SQL statement (disabled when `MODAK_CONSOLE_SQL=false`) |
| `/api/explain` | `POST` a SQL statement, returns the `modak_explain` report lines |
| `/metrics` | Prometheus text format, same as the headless worker |

`/metrics` is served by both binaries, so Prometheus scrapes either the same
way.

## In SQL instead

The same per-table picture is available without the console:

```sql
SELECT * FROM modak.status;
--  table_id | schema_name | table_name | mode | cutline_t | cutline_s |
--  mirror_frontier | delta_backlog | read_pins | copying | partition_states
```
