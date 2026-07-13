# Real-Time Lakehouse + AI Platform

One integrated system covering **Data Engineering**, **AI Engineering**, and
**System Design**: a live Bluesky firehose streamed through a Kafka-compatible
log and Flink into an Iceberg lakehouse, modeled with dbt into a Kimball star
schema, served sub-second from ClickHouse behind a live dashboard, with a
LangGraph AI agent and a full production layer (evals, tracing, load tests) on top.

**Status: Phase 0** — foundation and design doc. See
[docs/build_plan.md](docs/build_plan.md) for the full plan and
[docs/design_decisions.md](docs/design_decisions.md) for locked design decisions.

## Architecture

```
[Live firehose] -> [Kafka/Redpanda] -> [Flink jobs] -> [Iceberg lakehouse on MinIO]
   (WebSocket)      durable log,       windowed aggs,   raw + curated tables,
                    partitioned        watermarks,      partitioned, time-travel
                    topics             exactly-once            |
                                                               v
[Postgres OLTP] -> [Debezium CDC] ------------------> (dims into same lakehouse)
                                                               |
                                                               v
                                   [dbt: staging -> marts, Kimball star schema, SCD2]
                                                               |
                                                               v
                                   [ClickHouse: materialized views, sub-second serving]
                                            |                          |
                                            v                          v
                          [Next.js live dashboard]        [Vector store: Qdrant/pgvector]
                                                                       |
                                                                       v
                          [LangGraph agent: text-to-SQL + hybrid RAG, MCP tools, memory]
                                                                       |
                                                                       v
                          [Serving: vLLM/API + routing + semantic cache + queue + breaker]
                                                                       |
                                                                       v
                          [Evals: Ragas + LLM-judge + CI gate]  [Langfuse traces + cost]
                                                                       |
                                                                       v
                          [k6 load tests + capacity planning + failure postmortem]
```

## Layout

| Directory | Purpose |
|---|---|
| `ingestion/` | Firehose producer(s) |
| `streaming/` | Flink jobs |
| `transforms/` | dbt project |
| `orchestration/` | Dagster |
| `dashboard/` | Next.js app |
| `agent/` | LangGraph agent + MCP tools |
| `docs/` | Plan, design decisions, postmortems |

## Running

Everything runs in Docker Compose:

```
make up      # start the stack
make logs    # follow logs
make down    # stop
make clean   # stop + remove volumes and local data
```
