# Learn Stream Processing & Analytics in TypeScript

A hands-on, runnable project for understanding real-time analytics at a Staff/EM
level — windows and watermarks, the probabilistic-counting toolbox, and rollup
cubes, all from scratch.

Every phase is a small script you can run and read. No build step: modern Node
runs the TypeScript directly. No external services.

> Built to match a Staff-level study path. The through-line: **analytics is a
> counting problem wearing different costumes.** Three recurring moves: **event
> time ≠ processing time** (watermarks decide when a window is done); **rollup,
> never recount** (the counting cube); **freshness is a priced menu**
> (batch-exact vs streaming-approximate).

## Setup

```bash
npm install   # dev types only
```

## The lessons

| Command | What you learn |
|---|---|
| `npm run phase1` | **Windows** — tumbling, sliding, and session windows over a stream |
| `npm run phase2` | **Event time & watermarks** — out-of-order events, closing windows, late data |
| `npm run phase3` | **Count-Min Sketch** — approximate frequency / top-K in fixed memory |
| `npm run phase4` | **HyperLogLog** — count unique visitors in a few KB |
| `npm run phase5` | **Bloom filter** — set membership with no false negatives |
| `npm run phase6` | **Rollup cube** — raw → minute → hour, "rollup never recount" |
| `npm run phase7` | **Exactly-once** — checkpoint + replay; a non-idempotent sink double-counts, an idempotent upsert converges |
| `npm run phase8` | **Stream-table duality** — changelog ⇄ table, log compaction, CDC (a WAL is a stream) |
| `npm run phase9` | **Stream joins** — windowed attribution + the temporal/as-of enrichment trap (SCD) |
| `npm run phase10` | **Backpressure** — credit-based flow control; lag grows safely vs a naive consumer's OOM |
| `npm run phase11` | **Lambda vs Kappa** — the two-codebase drift tax vs one replayable log |
| `npm run phase12` | **Schema evolution** — the registry; add-optional is safe, a rename is rejected |

> **Phases 7–12 fold in the Staff-level cruxes** the module is built around —
> exactly-once (a property of the *effect on state*, not delivery), stream-table
> duality + CDC, stream joins, backpressure, the Lambda-vs-Kappa drift tax, and
> schema evolution. All dependency-free and deterministic.

## What each phase proves (the money quotes)

- **Phase 2** — an event that arrives out of order still lands in the right
  window (bucketed by *event* time), while one arriving after its window closed
  on the watermark is correctly flagged **late**.
- **Phase 3** — frequencies of the hot tags among 300k events estimated to within
  **<1%** using a fixed **40 KB** sketch instead of a 50k-entry map.
- **Phase 4** — **200,000** unique users counted from 1M events in **4 KB**
  (~2–3% error) — and HLLs merge across shards.
- **Phase 5** — **zero** false negatives, and a measured false-positive rate
  (0.83%) that matches the textbook formula (0.82%).
- **Phase 6** — the hourly count equals the sum of its minute buckets equals a
  raw recount, exactly — so you build each grain once and never rescan the firehose.
- **Phase 7** — the same crash + replay leaves a `+= 1` counter at **13** (double
  counted) but an idempotent upsert **converges to 10** — exactly-once is a
  property of the effect on state, only as strong as the sink.
- **Phase 9** — an order at event-time T enriched with the price valid *then*
  ($10) is reproducible; a naive "current" lookup ($20) is wrong on replay — the
  slowly-changing-dimension trap.
- **Phase 11** — under Lambda the batch layer counts a late event (**101**) and
  the speed layer drops it (**100**); the two drift. Kappa replays one log to the
  same **101**, no second codebase.

## Interactive Stream Lab

Every phase is also a live, **drawn** instrument in the browser — slide a window
over a timeline, advance a watermark until a straggler goes late, watch a Count-Min
sketch over-count, crash-and-replay to see a sink double-count, fold a changelog
into a table, and watch a slow sink turn into safe Kafka lag instead of an OOM.

```bash
npm run web        # serves web/index.html at http://localhost:8080 (no deps)
```

One self-contained static page (SVG visuals, self-hosted fonts), grouped by tier
and deep-linkable. To host it on **Cloudflare Pages**: connect this repo in the
dashboard with build output `web` (auto-deploys on push), or run
`npx wrangler login` then `npm run deploy`.

## The probabilistic toolbox (which structure answers which question)

| Structure | Question | You accept |
|---|---|---|
| **Count-Min Sketch** | "how many times did X appear?" | slight over-count |
| **HyperLogLog** | "how many DISTINCT items?" | ~1–2% error |
| **Bloom filter** | "have I seen X?" | some false positives, never false negatives |

## Project layout

```
src/
  lib/log.ts   (logger + shared hash)
  phase1/  windows (tumbling / sliding / session)
  phase2/  event time + watermarks + late events
  phase3/  Count-Min Sketch
  phase4/  HyperLogLog
  phase5/  Bloom filter
  phase6/  rollup counting cube
  phase7/  exactly-once (checkpoint + idempotent sink)
  phase8/  stream-table duality + CDC
  phase9/  stream joins (windowed + temporal as-of)
  phase10/ backpressure (credit-based flow control)
  phase11/ Lambda vs Kappa
  phase12/ schema evolution + the registry
web/
  index.html  ·  serve.mjs   (the interactive Stream Lab — npm run web)
```

## License

MIT — use it, fork it, learn from it.
