/** Drill data — mined from Core Course/16-data-processing.md (+34). Loaded by index.html's Drill panel. */
window.DRILL = {
  module: "Module 16 — Data Processing & Streaming",
  source: "Core Course/16-data-processing.md + 34-design-realtime-analytics.md",
  cheats: [
    "Batch and stream are the <b>same computation over bounded vs unbounded data</b> — the Beam thesis.",
    "<b>Default to batch unless a stated latency requirement forces streaming</b> — most 'real-time' is really 'within a few minutes'.",
    "<b>Event time</b> gives you <em>reproducibility</em>; processing time doesn't — replay the log a year later, get identical windows.",
    "A <b>watermark</b> is a heuristic latency/completeness <em>knob</em>, not a correctness guarantee — pair it with allowed lateness + a late-data side output.",
    "<b>Exactly-once</b> is a property of the <em>effect on state</em>, not message delivery — and only as strong as your sink (idempotent upsert or 2PC).",
    "Default to <b>at-least-once + idempotent upserts</b>; reserve transactional 2PC for money movement.",
    "<b>Backpressure is a feature</b>: it trades latency for stability so you never lose data — Kafka's durable log makes graceful degradation possible.",
    "Every <b>table is a stream and every stream is a table</b> — CDC, materialized views, and event sourcing all follow from the duality.",
    "<b>Spark</b> is a batch engine that streams via micro-batch; <b>Flink</b> is a streaming engine that does batch as a bounded stream.",
    "<b>ELT won</b> because cloud warehouses decoupled storage from compute and made raw retention cheap — but mask PII <em>before</em> landing.",
    "<b>Lambda's</b> flaw is two codebases that drift; <b>Kappa</b> replays one log — run <em>unified-code Kappa</em> (same pipeline as bounded batch and unbounded stream).",
    "<b>Lakehouse</b> = lake economics + warehouse ACID via Iceberg/Delta; <b>data mesh</b> is an <em>org</em> pattern, not a tool you buy.",
    "<b>Columnar wins</b> for three compounding reasons: read fewer columns, compress typed data 10x+, vectorized SIMD execution.",
    "<b>CDC</b> (Debezium reading the WAL) retires dual writes and nightly dumps — but watch the Postgres <em>replication-slot disk footgun</em>.",
    "<b>Schema evolution</b> with a registry + backward compatibility lets fifty teams ship independently — a field add is safe, a <em>rename is a migration</em>."
  ],
  cards: [
    {
      topic: "batch vs stream",
      q: "What's the real difference between batch and stream processing, and how do you decide?",
      a: "It's bounded vs unbounded data — latency, cost, and completeness fall out of that. Batch waits for complete data: highest throughput, cheapest $/TB, minutes-to-hours latency. Stream processes unbounded data per-event in milliseconds but must reason about incompleteness via watermarks. Default to batch; escalate to streaming only when a stated requirement forces a business action in seconds — streaming-by-reflex is a 5-10x cost mistake. Micro-batch (Spark) is the middle ground."
    },
    {
      topic: "watermarks",
      q: "Walk me through event time, watermarks, and late data.",
      a: "Event time is when it happened (the truth, in the payload); processing time is when you handle it — bucket by event time for reproducibility. A watermark asserts 'I've seen everything up to W': a heuristic trading latency for completeness, never a correctness guarantee. When it passes a window, the window fires; allowed lateness keeps state around to re-fire on stragglers, and truly-late events go to a side output reconciled in batch — the provisional-then-final pattern. Re-fires mean the sink must be upsert-by-key and downstream must tolerate restatements. Gotcha: one idle partition stalls the global watermark — you need idle-source detection."
    },
    {
      topic: "exactly-once",
      q: "What does exactly-once actually mean, and how is it achieved?",
      a: "It's effectively-once: each event affects state exactly once despite replays — not 'delivered once', which is impossible over a network. Flink gets it internally via Chandy-Lamport checkpoints (barriers snapshot state + Kafka offsets atomically). End-to-end requires an idempotent sink (upsert by deterministic key) or a transactional 2PC sink — claiming exactly-once without naming the sink mechanism is the hand-wave interviewers pounce on. Default to at-least-once + idempotent upserts; reserve 2PC for money."
    },
    {
      topic: "Spark vs Flink",
      q: "Spark or Flink — when and why?",
      a: "Spark is a batch engine that streams via micro-batch; Flink is a true streaming engine where batch is a bounded stream. The decision axis is latency and state complexity: seconds of freshness, ML, already on Databricks — Spark (and tuning is 80% minimizing and de-skewing shuffles). Sub-second latency, complex event-time sessionization, huge keyed state, CEP, safe stateful rescaling via savepoints — Flink. Both can be right; dogma is the wrong answer — state the default, name the trigger to switch, stop."
    },
    {
      topic: "ELT",
      q: "Why did ELT replace ETL, and what are its downsides?",
      a: "Cloud warehouses decoupled storage from compute and made raw retention cheap, so loading raw and transforming in-place (SQL/dbt) became cheaper, scalable, and reprocessable — and analysts can build transforms. Downsides: cost shifts into warehouse compute (un-pruned scans get expensive) and raw dumping risks a swamp without governance. Keep raw + transform-in-place for analytics, but mask PII and do heavy ML feature logic before landing."
    },
    {
      topic: "Lambda vs Kappa",
      q: "Lambda vs Kappa — which would you build today?",
      a: "Lambda's defining flaw is the duplication problem: the same logic in two engines that drift, producing inconsistent numbers between the live dashboard and the corrected report — a perpetual reconciliation tax. Kappa keeps one stream codebase and reprocesses by replaying the log, relying on modern event-time and exactly-once correctness. Pure Kappa struggles with massive historical recompute, so build unified-code Kappa: Beam/Flink where the same pipeline runs as bounded batch for backfills and unbounded stream for live. If you ever draw Lambda, name the drift tax in the same breath."
    },
    {
      topic: "CDC",
      q: "Explain CDC, how you'd use it, and what can go wrong.",
      a: "CDC streams row-level changes from the DB's WAL/binlog (Debezium) as ordered events — low source overhead, captures deletes, real-time. It's stream-table duality operationalized and retires dual writes and nightly dumps: the DB stays the single source of truth, one consistent stream feeds warehouse, search, and caches. Default to log-based, not polling — polling misses deletes. Footguns: a lagging consumer leaves the Postgres replication slot unconsumed, growing WAL until the disk fills and crashes the primary — monitor slot lag religiously — and pair with a schema registry."
    },
    {
      topic: "columnar",
      q: "Why is columnar storage faster for analytics?",
      a: "Three compounding reasons: read only the columns the query touches (analytics scans few columns over many rows); typed, repetitive columns compress 10x+ via dictionary/RLE/delta; and vectorized SIMD execution over column batches. That's why Parquet, Snowflake, and ClickHouse exist. Corollary: columnar is terrible at single-row updates — OLTP and OLAP are different physics, so never run dashboards on the transactional primary; CDC the changes into a columnar OLAP store."
    },
    {
      topic: "idempotency",
      q: "How do you make a data pipeline safe to retry and backfill?",
      a: "Idempotency + partition-scoping: each run owns a deterministic output slice keyed by its logical/event date and writes with overwrite or upsert-by-key, never append — so a retry replaces rather than duplicates, and a 90-day backfill is just re-running idempotent partitions. Use event-time, not wall-clock, semantics so historical runs are correct. The orchestrator (Airflow/Dagster) is a control plane that triggers Spark/dbt — it shouldn't crunch data itself."
    },
    {
      topic: "schema",
      q: "How do you evolve schemas across many teams without breaking everyone?",
      a: "Avro/Protobuf + a schema registry in backward-compatibility mode: producers embed a schema ID, consumers fetch by ID, and the registry rejects incompatible changes at registration. A field add (optional, with default) is safe with no coordinated deploy; a rename or type-narrowing is a breaking change the registry blocks — handle it as a migration (add new field, backfill, deprecate old). This is what lets fifty teams ship independently, and it's the governance counterpart to idempotency and CDC."
    },
    {
      topic: "pipeline design",
      q: "Design a real-time clickstream analytics pipeline.",
      a: "Pin the SLOs first: sub-5s dashboards force a streaming speed path, historical/ML is batch, mobile means out-of-order event-time, 'reprocess' means a replayable log. One durable Kafka log partitioned by session as the spine, Avro + schema registry at ingest; a Flink speed path (event-time, watermarks, TTL-capped sessionization, RocksDB state) upserts idempotently into a columnar OLAP store (ClickHouse/Druid/Pinot), late tail to a side output. The same log sinks idempotently to a lakehouse (Iceberg/Delta) transformed by dbt — and batch-only marts stay off the streaming path. Close with the SLOs: 5-second freshness, exactly-once-by-effect, graceful degradation to lag (never data loss), reproducible reprocessing."
    },
    {
      topic: "altitude",
      q: "Five streaming engines, three architectures — how do you keep an answer at the right altitude instead of touring them all?",
      a: "Lead with the decision and the requirement that drives it, not a catalog. Name the basics in one breath — 'Kafka in, process, OLAP store out' — and spend the time where design risk lives: event-time/watermarks/late data, the exactly-once sink mechanism, and the Lambda-vs-Kappa drift tax. For every fork say 'choice, because requirement; revisit if trigger' — and actively de-escalate: if a nightly batch meets the SLO, say so and don't reach for Flink."
    },
    {
      topic: "dedup",
      q: "How do you count clicks without double-counting when delivery isn't reliable?",
      a: "Delivery is at-least-once — retries and redeliveries make duplicates normal — so make counts effective-once by deduping on an event_id idempotency key: the processor keeps a TTL'd set of recently-seen ids (or a Bloom filter) and increments only on first sight. That's exactly-once effect on state via checkpointed state plus idempotent dedup. Be explicit that you never claim exactly-once delivery — that doesn't exist over a network."
    },
    {
      topic: "approximate counting",
      q: "When do you use approximate counting, and what's the error?",
      a: "Where a small, known error is free. Unique users per (ad x window) as an exact set is GB-TB of memory; HyperLogLog does it in ~12 KB at ~0.8% standard error, and HLLs merge — a daily count is the merge of 24 hourly sketches. Top-K over a huge key space uses Count-Min Sketch: sub-linear memory, but it over-counts (one-sided error). The judgment is matching tool to metric: approximate on dashboards, exact and reconciled for anything that bills."
    }
  ]
};
