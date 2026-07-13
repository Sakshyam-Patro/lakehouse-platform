# Design Decisions — Phase 0

Decisions made by **Sakshyam Patro** on 2026-07-13 during the Phase 0 design interview.
These are locked. Do not revisit without an explicit decision to change them, recorded here.

---

## D1. Domain: Bluesky Jetstream

**Decision (Sakshyam):** The live data source is the **Bluesky Jetstream** firehose
(JSON over WebSocket, thousands of events/sec).

**Why:** Highest volume of the free options, visually interesting (trending topics,
activity waves, language breakdowns), highest viral potential for the public dashboard.
The producer layer stays swappable so nothing downstream depends on the source.

## D2. Fact table and grain: `fct_events`

**Decision (Sakshyam):** One fact table, `fct_events`, at the atomic grain:

> **One row per Jetstream *commit* event** — a create, update, or delete of a record
> (post, like, repost, follow, block, …) — uniquely identified by the composite key
> **(did, collection, rkey, operation, time_us)**.

Clarifications made during the interview (grain was deliberately sharpened):

- **Commit events only.** Jetstream `identity` and `account` messages are *not* fact
  rows; they feed dimensions (notably `dim_user`).
- **Deletes and updates are fact rows** (operation is part of the grain), enabling
  deletion analytics.
- **The composite key de-duplicates cursor replays** on WebSocket reconnect —
  ingestion must be idempotent against this key.

**Why atomic grain:** finest grain preserves all future rollups (per-minute, per-entity
aggregates are derived in ClickHouse/dbt, never the source of truth). Pre-aggregated
grains destroy drill-down ability permanently.

## D3. Dimensions

**Decision (Sakshyam):** Five dimensions around `fct_events`:

| Dimension | Source | Notes |
|---|---|---|
| `dim_date` / `dim_time` | Generated | Required by the plan; standard calendar/time-of-day attributes. |
| `dim_user` | Jetstream identity events + Postgres tracked-users table via Debezium | Actor DID, handle, display name, follower bucket. **SCD Type 2** (see D4). |
| `dim_event_type` | Postgres via Debezium | Maps collection strings (`app.bsky.feed.post`, `.like`, `.graph.follow`, …) to friendly names/categories. Simple CDC exercise. |
| `dim_language` | Derived from post `langs` | Enables the language-breakdown dashboard panel. |
| `dim_topic` | Curated Postgres table via Debezium | Tracked topics/hashtags maintained by hand; richer CDC source and a "tracked topics" dashboard story. |

## D4. SCD Type 2 dimension: `dim_user`

**Decision (Sakshyam):** `dim_user` gets SCD Type 2 (effective_from / effective_to /
is_current, via dbt snapshots).

**Why:** Handles and display names change organically in the live stream — genuine
history with zero manual effort — and it answers the classic point-in-time question:
*"what was this user's handle at the time of the event?"* `dim_event_type` and
`dim_language` are near-static; `dim_topic` changes only artificially.

---

## Non-goals (Phase 0)

- No Kubernetes; Docker Compose is enough for one node.
- No Avro/Protobuf schema registry yet (JSON first; know why the alternatives exist).
- No cloud deployment until the public dashboard phase.
