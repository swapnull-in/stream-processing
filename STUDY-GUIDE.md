# Study Guide — Stream Processing & Real-Time Analytics

This repo is the runnable companion to **Core Course/16-data-processing.md** (plus the design case in **34-design-realtime-analytics.md**). Study loop: read a module section, run the matching phase (`npm run phaseN`) and predict the output before you look, then explain the staff insight out loud as if an interviewer asked. Finish with the web Drill panel (`npm run web`) for spaced active recall of the one-liners and Q&A.

## Phase → module mapping

| Phase | What it builds | Module section | The staff insight |
|---|---|---|---|
| 1 | Tumbling, sliding, and session windows over a stream | 16 §2.3 Windowing | "reformulate heavy sliding aggregates as incremental/cumulative computations to avoid the fan-out" |
| 2 | Event time vs processing time; a watermark closes windows, flags stragglers late | 16 §2.1–2.2, §2.4 | "a latency-vs-completeness knob you tune, not a correctness guarantee" |
| 3 | Count-Min Sketch: frequencies/top-K in fixed memory | 34 §8.3 Counting at scale | "Count-Min Sketch: sub-linear memory, but it over-counts (one-sided error)" |
| 4 | HyperLogLog: distinct counts in a few KB, mergeable across shards | 34 §8.3 Counting at scale | "HyperLogLog does it in ~12 KB at ~0.8% standard error, and HLLs merge" |
| 5 | Bloom filter: set membership, no false negatives | 34 §13 Q1 (dedup) | "a TTL'd set of recently-seen ids (or a Bloom/Cuckoo filter), incrementing only on first sight" |
| 6 | Rollup cube: raw → minute → hour, each grain built once | 34 §6/§8.5 Serving | "pre-aggregate in the stream and serve compact rollups from a columnar real-time OLAP store" |
| 7 | Checkpoint + replay; a naive sink double-counts, an idempotent upsert converges | 16 §2.7 Exactly-Once | "Exactly-once is a property of the effect on state, not of message delivery" |
| 8 | Changelog ⇄ table folding, log compaction, CDC from a WAL | 16 §2.9, §6.4 | "Every table is a stream and every stream is a table — CDC, materialized views, event sourcing all follow" |
| 9 | Windowed stream-stream attribution + temporal as-of enrichment | 16 §2.10 Joins | "join against the dimension value as-of the event's event-time, not the latest" |
| 10 | Credit-based flow control; lag grows safely vs a naive consumer's OOM | 16 §2.8 Backpressure | "It's the system trading latency for stability so you never lose data" |
| 11 | Lambda's two drifting codepaths vs Kappa's one replayable log | 16 §6.1 Lambda vs Kappa | "the same logic in two engines that inevitably drift; Kappa bets the log is replayable" |
| 12 | A schema registry that accepts add-optional and rejects a rename | 16 §6.5 Schema Evolution | "a field add is safe, a rename is a migration" |

## Go deeper

- **Deep Dives/07-flink.md** — the engine that productized phases 2, 7, and 10: watermarks, Chandy-Lamport checkpoints, RocksDB state, savepoints.
- **Deep Dives/02-kafka.md** — the durable, replayable, partitioned log every phase assumes as its source; partitioning, ordering, at-least-once.
- **Deep Dives/20-clickhouse.md** — the columnar OLAP serving layer the rollup cube (phase 6) feeds; why scans and pre-aggregation beat COUNT(*).
- **Deep Dives/27-topk-music-analytics.md** — Count-Min Sketch + heap (phase 3) deployed as a full top-K design under interview conditions.
- **DDIA ch. 11 (Batch Processing)** — MapReduce, shuffle-and-sort, and why idempotent, re-runnable batch is the baseline streaming must beat.
- **DDIA ch. 12 (Stream Processing)** — the book-form treatment of duality, CDC, event time, and exactly-once that phases 2 and 7–9 dramatize.
