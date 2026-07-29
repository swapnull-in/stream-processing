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
```

## License

MIT — use it, fork it, learn from it.
