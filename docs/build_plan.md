# The Flagship Build: Real-Time Lakehouse + AI Platform

One integrated system covering **Data Engineering**, **AI Engineering**, and **System Design**.
Owner: Sakshyam Patro. Timeline: ~12–16 weeks part-time, July 2026 → October 2026.

> **Note for Claude Code:** This is the master build plan. The current phase and working
> agreements live in `CLAUDE.md` at the repo root. Never build ahead of the current phase.
> For anything involving streaming semantics, data modeling, retrieval design, or evals:
> explain the approach and tradeoffs FIRST and wait for approval before writing code.

---

## 1. What we are building and why it is one system

A platform where data flows end to end through every layer a real company runs:

1. A **live public firehose** (thousands of events/sec) is ingested into a durable log (Kafka/Redpanda).
2. **Stream processing** (Flink) computes real-time aggregations with correct time semantics and exactly-once guarantees.
3. Events land in a **lakehouse** (Apache Iceberg on MinIO/S3), alongside change-data-captured records from an operational Postgres database (Debezium).
4. **Batch transformation** (dbt) models raw data into a Kimball star schema with slowly changing dimensions.
5. A **real-time OLAP engine** (ClickHouse) serves sub-second analytical queries behind a live, public, interactive dashboard (Next.js/React).
6. An **AI agent** (LangGraph) sits on top: text-to-SQL against ClickHouse for analytical questions, hybrid retrieval against a vector store for semantic questions, tools exposed over MCP.
7. A **production layer** wraps everything: model serving and routing, semantic caching, queues, rate limits, circuit breakers, an evaluation harness with CI gates, tracing and cost observability, load testing, and deliberate failure postmortems.

**Why one integrated system instead of three projects:**

- System design is the study of how components behave together under load. Backpressure, cache invalidation, queue depth, graceful degradation — none of these exist in a toy project.
- It mirrors real production architectures that interview questions are drawn from.
- One coherent story and one live demo instead of three shallow repos.
- Compounding efficiency: one dataset, one Docker Compose stack, one repo.

Each phase is **independently demoable and resume-worthy**, so value is banked early (August 2026 graduation, OPT clock).

---

## 2. Step zero: choose the domain

Requirements: free, genuinely high-volume, real-time, visually interesting.

| Option | What it gives you | Vibe |
|---|---|---|
| **Bluesky Jetstream** | Social firehose, JSON over WebSocket, thousands of events/sec. Trending topics, activity waves, language breakdowns. Highest viral potential. | Consumer / social |
| Crypto exchange feeds (Coinbase/Binance public WS) | Extreme volume, natural anomaly-detection stories. | Quant / markets |
| Wikimedia EventStreams | Every edit across Wikimedia, free SSE. Extremely reliable and clean. | Knowledge / stable |
| GitHub public events | Pulse of open source. | Dev tools |

**Recommendation: Bluesky Jetstream.** Keep the producer layer swappable so nothing downstream cares about the source. **Decide before Phase 1.**

---

## 3. Architecture at a glance

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

Everything runs locally in **Docker Compose** on a 16GB+ laptop for ~$0. Small optional cloud spend (Section 12) buys the "runs in production" credential.

---

## 4. Skill coverage by pillar

| Pillar | What this project forces you to learn |
|---|---|
| **Data Engineering** | Kafka topics/partitions/consumer groups; Flink windows, event time vs processing time, watermarks, stateful processing, exactly-once; Iceberg partitioning, schema evolution, snapshots/time-travel, compaction; Debezium log-based CDC, upserts, MERGE; dbt staging→marts layering, tests, snapshots; Kimball dimensional modeling, fact grain, SCD Type 2; Dagster software-defined assets, lineage, sensors, partitioned assets; ClickHouse MergeTree, materialized views, denormalization; data contracts and quality gates. |
| **AI Engineering** | Chunking strategies and embedding models; hybrid retrieval (BM25 + dense) with reciprocal rank fusion and a cross-encoder reranker; LangGraph state machines, tool-calling, memory; text-to-SQL with schema grounding; MCP servers; golden sets, Ragas metrics (faithfulness, context precision/recall, answer relevancy), LLM-as-judge, CI regression gates; vLLM continuous batching, PagedAttention, quantization; model routing; Langfuse tracing and cost accounting. |
| **System Design** | The seven patterns interviewers probe: queue-backed worker pools, circuit breakers, caching layers, rate limiting, load balancing, autoscaling logic, graceful degradation. Plus batch/stream duality, backpressure, idempotency, load testing with k6/Locust, and capacity-planning math (QPS, storage growth, p50/p99 latency, cost per query). |

---

## 5. Phase 0 — Foundation and design doc (2–3 days)

### Goals
Lock the architecture on paper before any code. **This document is hand-designed (Zone 1) work — the human writes it, not Claude Code.**

### Tasks
1. Write `docs/design_decisions.md`: architecture diagram, data model sketch, every tech choice with a one-line justification, explicit non-goals (e.g., "no Kubernetes; Compose is enough for one node").
2. Sketch the **star schema** on paper: What is the fact table? What is its **grain** (one row per event? per minute per entity?)? Which dimensions? Which dimension gets SCD Type 2?
3. Create the repo with a **CLAUDE.md** at the root: project overview, architecture summary, conventions (naming, directory layout), what is decided vs. open, and the working agreements.
4. Stand up minimal **Docker Compose** with service placeholders and a Makefile (`make up`, `make down`, `make logs`).
5. Initialize git; commit checkpoints aggressively from day one.

### Deliverables
Design doc + diagram in the README; empty-but-running Compose stack.

### Why it matters in interviews
"I wrote a design doc, made these tradeoff calls, then executed in phases" is how senior engineers operate. The doc itself is an artifact to show.

---

## 6. Phase 1 — Streaming + batch lakehouse foundation (3–4 weeks)

The data-engineering core and the densest learning phase.

### Week 1 — Ingestion into the log
- Write the **producer**: connect to the firehose WebSocket, publish JSON events to **Redpanda** (Kafka-compatible, single binary, easiest locally).
- Learn deliberately: topics, **partitioning strategy** (what key? why? what happens with a hot key?), serialization (JSON now; know why Avro/Protobuf + schema registry exist), consumer groups and rebalancing.
- **Milestone:** `kafka-console-consumer` shows a steady live stream; producer survives a WebSocket disconnect (reconnect logic).

### Week 2 — Stream processing with correct time semantics
- Write **Flink** jobs (PyFlink or Java) consuming the topic: a tumbling-window count per entity, a sliding-window trending metric, a session-window example.
- Learn the concepts interviewers love: **event time vs processing time**, **watermarks** (how late data is handled — test by replaying delayed events), **stateful processing**, **checkpointing**, and **exactly-once** via the two-phase-commit sink.
- **Milestone:** kill the Flink taskmanager mid-stream; verify the job recovers from a checkpoint with no loss and no duplicates. Write down what you observed — postmortem material.

### Week 3 — Lakehouse + CDC
- Sink raw and curated streams to **Apache Iceberg** tables on **MinIO**. Learn: hidden partitioning, **schema evolution** (add a column live; nothing breaks), **snapshots and time-travel** (query the table as of an hour ago), compaction of small files.
- Add a small **Postgres** OLTP table (tracked entities/categories — the dimension source) and stream its changes into the lakehouse via **Debezium**. Learn: log-based replication vs polling, tombstones, upsert/MERGE into Iceberg.
- **Milestone:** update a row in Postgres; watch the change appear downstream without a batch job.

### Week 4 — Modeling, transformation, orchestration, quality
- **dbt** over the Iceberg tables: `staging → intermediate → marts`. Implement the star schema designed in Phase 0: at least one **fact table** with an explicit grain statement in the model docs, a `dim_time`, and one **SCD Type 2** dimension via dbt snapshots.
- Add **dbt tests** (unique, not-null, relationships, accepted values) plus one Great Expectations or Soda check as a hard **quality gate**.
- Orchestrate with **Dagster**: software-defined assets for every table, schedules for batch models, a sensor watching the stream, partitioned assets by day. Screenshot the lineage graph for the README.
- **Milestone:** one command (`dagster dev` + `make up`) brings up the entire pipeline from firehose to tested marts.

### Concepts to whiteboard cold after this phase
Fact-table grain and why it was chosen; SCD2 mechanics (effective-from/to, current flag); exactly-once (what actually guarantees it end to end); watermark tradeoffs (lateness vs latency); why Iceberg over raw Parquet (ACID, evolution, time-travel).

### Pitfalls
- Do not skip the grain statement. "One row per X" ambiguity is the #1 data-modeling interview failure.
- Small-file explosion in Iceberg from streaming writes — learn compaction now, not later.
- The human chooses partition keys, not Claude Code. Be ready to defend the choice.

### Deliverables and resume bullet
Running Compose stack; documented star schema; Dagster lineage screenshot.

> Built a streaming pipeline (Kafka, Flink, exactly-once) landing a live firehose into an Apache Iceberg lakehouse with Debezium CDC; modeled a Kimball star schema (SCD Type 2) in dbt with full test coverage, orchestrated as Dagster software-defined assets.

### Interviews this phase prepares
Meta DE data modeling and ETL rounds; any streaming-semantics question; startup "walk me through a pipeline you own."

---

## 7. Phase 2 — Real-time OLAP + interactive dashboard (2 weeks)

The serving layer and the shareable, potentially viral artifact.

### Week 5 — ClickHouse serving layer
- Load the dbt marts into **ClickHouse**; build **materialized views** for pre-aggregated rollups (per-minute, per-hour, per-entity).
- Learn: columnar storage intuition, the MergeTree engine family, ORDER BY as the primary index, when to denormalize and what it costs.
- **Milestone:** p95 of dashboard queries under 200ms on a laptop with continuous ingestion running.

### Week 6 — The dashboard
- Build the **Next.js/React** dashboard: live-updating headline metrics, trending entities, time-series charts, drill-downs. WebSocket or short-polling for the live feel. This is the D3/React strength — make it beautiful.
- Deploy publicly: Vercel front-end; small VM or tunnel for the backend. It must have a URL a stranger can open.
- Post it (Reddit, HN, LinkedIn). Front-load the shareability.

### Pitfalls
Do not query the lakehouse directly from the dashboard (that is what the serving layer is for). Do not over-poll ClickHouse — batch the dashboard's queries.

### Deliverables and resume bullet
A live public dashboard URL.

> Served sub-second analytics on live streaming data via ClickHouse materialized views behind a public interactive React dashboard.

---

## 8. Phase 3 — The AI layer: agent over your own data (3–4 weeks)

AI engineering grounded in the platform's own live pipeline — the differentiator versus generic chatbot wrappers.

### Week 7 — Vector ingestion
- A **Dagster asset** that chunks and embeds text from the stream (or related documents) into **Qdrant** or **pgvector**.
- Compare two **chunking strategies** and two embedding models; keep notes — eval material for Phase 4.

### Weeks 8–9 — The agent
- A **LangGraph** agent with two data planes and a router:
  - Analytical questions → **text-to-SQL** against ClickHouse ("top 10 trending topics in the last hour"). Ground with the schema and worked examples; **validate generated SQL before execution** (read-only ClickHouse user); retry on failure.
  - Semantic questions → **hybrid retrieval**: BM25 + dense vectors fused (reciprocal rank fusion), then a **cross-encoder reranker**.
- Expose tools via **MCP**. Add conversation memory and loop/termination guards.
- Embed the agent in the dashboard as a copilot panel.

### Week 10 — Experimentation module (the Meta DS angle)
- Assign incoming events (or dashboard sessions) to variants; compute **metric lift with significance testing** (two-proportion z-test or bootstrap); render an experiment-results panel.
- This module converts the biggest Meta DS Product Analytics gap (no A/B evidence) into a discussable artifact.

### Pitfalls
Text-to-SQL without validation will eventually run a bad query — sandbox with a read-only user. Retrieval quality, not the LLM, is the usual failure: measure it before blaming the model.

### Deliverables and resume bullet
Live AI copilot in the dashboard; experiment panel.

> Built a LangGraph agent over the platform: hybrid retrieval (BM25 + dense + reranker) for semantic queries and validated text-to-SQL over ClickHouse for analytical queries, with MCP tool-calling and memory.

---

## 9. Phase 4 — Production: serving, evals, observability, scale (3–4 weeks)

The credential layer — the 30% most people skip, and the part that actually impresses.

### Week 11 — Serving and routing
- Self-host an open model with **vLLM** for a benchmark window (continuous batching, PagedAttention, quantization), *or* stay on APIs and produce a documented **cost/latency comparison** across providers.
- Add **model routing**: cheap model for triage/classification, frontier model for hard reasoning. Log which route each request took.

### Week 12 — The seven patterns, deliberately
- **Queue-backed worker pool** for batch embedding jobs (watch queue depth under burst).
- **Semantic + embedding caching** (measure hit rate; report cost saved).
- **Rate limiting** on the agent endpoint (denial-of-wallet defense).
- **Circuit breaker** around the LLM and the vector store; force a downstream failure and watch it open.
- **Graceful degradation**: when the vector store is down, the agent answers analytical questions only and says so.

### Week 13 — Evals and observability
- Build a **golden set** (50–100 question/answer pairs across both data planes).
- Run **Ragas** (faithfulness, context precision/recall, answer relevancy) plus an **LLM-as-judge** rubric; wire both into **CI** so a quality regression blocks the merge (DeepEval or Braintrust).
- Instrument everything with **Langfuse**: traces, token counts, cost per request, latency percentiles. Screenshot the cost dashboard.
- Produce one **before/after table** (e.g., retrieval precision with vs without the reranker; hallucination rate before vs after grounding changes).

### Week 14 — Load, capacity, failure
- **Load-test** with k6 or Locust: find the p99 knee, document throughput at saturation.
- Do the **capacity-planning math** in the README: events/sec, storage growth/day, QPS the serving layer sustains, cost per 1k agent queries.
- **Induce failures on purpose** (kill Flink, kill the vector store, saturate the queue) and write a real **postmortem**: what broke, how it recovered, what changed as a result.

### Deliverables and resume bullet
Benchmark + eval report with charts; cost dashboard screenshot; postmortem write-up.

> Added an eval harness (Ragas + LLM-as-judge) gating CI on regressions, Langfuse tracing and cost dashboards; load-tested to a documented p99 and cost-per-query; wrote failure postmortems for induced outages.

---

## 10. Hand-build vs AI-assist vs delegate

| Zone | Contents |
|---|---|
| **Zone 1: hand-design / hand-build** | Every architecture decision; the star schema (know the fact grain cold); Flink streaming semantics; retrieval + eval design; placement of queues, caches, breakers. Build one component per pillar deeply by hand: suggested — the Flink job, the eval harness, the retrieval pipeline. |
| **Zone 2: AI-assist, full review** | dbt SQL after the schema is designed; Dagster boilerplate; the React dashboard; Dockerfiles; FastAPI scaffolding. Every diff read like a reviewer; reject and redirect freely. |
| **Zone 3: delegate freely** | Config, glue, test scaffolding, README polish, CSS. |

**Guardrails:**
- When something breaks in Zone 1–2 territory, the human forms a hypothesis for 15–20 minutes before pasting the error into Claude — debugging is where system intuition forms.
- After any significant AI-generated chunk lands: close the file and explain it back in three sentences. If that fails, slow down on that component.
- End sessions with a quiz: Claude asks the human 3 questions about what was just built.

---

## 11. Framing the same project for each audience

- **Big tech (Meta DE, Databricks, Snowflake):** lead with the star schema + SCD2, exactly-once semantics, and benchmark numbers (throughput, p99 under load, cost per query). Emphasize correctness and how to scale it 10–100×.
- **Meta DS Product Analytics:** lead with the experimentation module (variant assignment, lift, significance), metric definitions, and the text-to-SQL analytical agent.
- **Startups (YC / a16z):** lead with the live demo URL, "shipped end-to-end solo in N weeks," full-stack ownership, and the eval traces + cost dashboard + postmortem — the strongest startup signals.

---

## 12. Cost

- **Default ~$0:** the entire stack in Docker Compose on a 16GB+ laptop — Redpanda, Flink, Iceberg on MinIO, dbt, Dagster, ClickHouse, Qdrant/pgvector, self-hosted Langfuse, Ragas.
- **Optional $20–100, high ROI:** a few hours of serverless GPU (Modal/RunPod) for the vLLM benchmark; managed vector-DB free tier; Databricks Free Edition for lakehouse credibility; ClickHouse Cloud free tier; a small VM for the public backend.
- Use LLM APIs for day-to-day agent development; reserve self-hosted vLLM for the benchmark window.

---

## 13. Sequencing toward August 2026 and beyond

1. **Weeks 1–6:** Phase 0 → 1 → 2. Ship the dashboard publicly and post it. The moment Phase 1 runs, the lakehouse resume bullet is real and defensible if a recruiter calls fast.
2. **Weeks 7–14:** Phases 3 → 4. The instant the eval suite + cost dashboard + postmortem exist, update every resume version — highest-signal artifacts on both tracks.
3. **If time compresses:** Phases 1 + 3 + a slice of 4 (evals + one benchmark) still cover all three pillars. Drop Phase 2's front-end polish last.
4. **Only if targeting senior/infra roles:** add a feature store (Feast) or Flink CEP. Skip for generalist new-grad applications.

---

## 14. The artifacts checklist (what actually impresses)

- [ ] Live public dashboard URL with real data flowing
- [ ] README with a screenshot/GIF at the very top and a live-demo link
- [ ] Benchmark write-up: throughput, p50/p99, cost per 1k queries
- [ ] Eval report: golden set, Ragas metrics, a before/after table
- [ ] Cost dashboard screenshot (Langfuse)
- [ ] Architecture diagram
- [ ] Failure postmortem (induced outage, documented recovery)
- [ ] Design doc showing tradeoff reasoning

The build is the fun part; this checklist is the top-few-percent part. Budget genuine time for it.
