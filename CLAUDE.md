# CLAUDE.md — Real-Time Lakehouse + AI Platform

## Current phase: PHASE 0
(Update this marker as phases complete. **Never build ahead of it.**)

## Project overview

One integrated system covering Data Engineering, AI Engineering, and System Design,
built by Sakshyam Patro as a learning project (~12–16 weeks, July → October 2026).
The master plan lives in `docs/build_plan.md`; locked decisions in
`docs/design_decisions.md`.

Architecture summary:

1. Bluesky Jetstream firehose (JSON/WebSocket) → **Redpanda** (durable log, partitioned topics)
2. **Flink** stream processing: windowed aggregations, watermarks, exactly-once
3. **Iceberg lakehouse on MinIO**: raw + curated tables, time-travel; plus
   **Postgres → Debezium CDC** landing dimension sources in the same lakehouse
4. **dbt**: staging → marts, Kimball star schema, SCD Type 2 via snapshots
5. **ClickHouse**: materialized views, sub-second serving → **Next.js live dashboard**
6. **LangGraph agent**: text-to-SQL over ClickHouse + hybrid RAG over a vector store, MCP tools
7. Production layer: serving/routing, semantic cache, queue, rate limits, circuit
   breakers, Ragas/LLM-judge evals with CI gates, Langfuse tracing, k6 load tests

## How I want to work

I (Sakshyam) am building this **to learn**. Rules for Claude:

- For anything involving **streaming semantics, data modeling, retrieval design, or
  evals**: explain the approach and tradeoffs FIRST, wait for my approval, then implement.
- **Small scoped tasks only.** Never build ahead of the current phase marker above.
- **I make the architecture decisions; you implement against them.**
- Prefer **boring, readable code** over clever code.
- At the **end of each session, quiz me with 3 questions** about what we built.

## Decided — do not revisit (from docs/design_decisions.md)

- **Domain:** Bluesky Jetstream firehose; producer layer stays source-swappable.
- **Fact table & grain:** `fct_events` — one row per Jetstream **commit** event
  (create/update/delete of a record), unique key **(did, collection, rkey, operation,
  time_us)**. Identity/account messages feed dimensions only. Ingestion must be
  idempotent against cursor replays.
- **Dimensions:** `dim_date`/`dim_time` (generated), `dim_user` (identity events +
  Postgres/Debezium), `dim_event_type` (Postgres/Debezium), `dim_language` (from post
  `langs`), `dim_topic` (curated Postgres table via Debezium).
- **SCD Type 2:** `dim_user` (effective_from/to, is_current, dbt snapshots).
- **Non-goals:** no Kubernetes (Compose only); JSON serialization for now (no schema
  registry yet); no cloud deploy until the dashboard phase.

## Conventions

- **Python 3.12** for all Python code.
- **Docker Compose** for all services; nothing runs bare on the host.
- **Makefile** targets: `make up`, `make down`, `make logs`, `make clean`.
- Directory layout:
  - `/ingestion` — firehose producer(s)
  - `/streaming` — Flink jobs
  - `/transforms` — dbt project
  - `/orchestration` — Dagster
  - `/dashboard` — Next.js app
  - `/agent` — LangGraph agent + MCP tools
  - `/docs` — plan, design decisions, postmortems
