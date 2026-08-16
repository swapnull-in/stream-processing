/**
 * Phase 11 — LAMBDA vs KAPPA ARCHITECTURE (§6.1). Run: node "src/phase11/lambda-vs-kappa.ts"
 *
 * The crux architecture decision for a streaming analytics stack. You have one
 * immutable, replayable EVENT LOG (the source of truth) and you want an
 * aggregation over it (here: count per user). Two philosophies:
 *
 *   LAMBDA — run TWO parallel paths and merge at query time:
 *     • BATCH layer — accurate but slow. Reprocesses the whole log, handles
 *       late / out-of-order data correctly. This is "the truth", eventually.
 *     • SPEED layer — fast but approximate. Serves fresh numbers NOW, but cuts
 *       corners (drops late events, rounds, uses sketches) to stay real-time.
 *     A serving layer merges the two. Its DEFINING FLAW: the same business logic
 *     lives in TWO codebases in TWO engines. The moment you change one and forget
 *     the other, they DRIFT and report inconsistent numbers. That is the
 *     DUPLICATION / DRIFT TAX — you pay it forever, on every logic change.
 *
 *   KAPPA — ONE path. Everything is a stream over the immutable log. To correct
 *     or reprocess, you REPLAY the log from the start through the SAME code. No
 *     second codebase to drift. A fresh run and a replay give the identical
 *     number, because the result is a pure function of the event-time log.
 *
 *   UNIFIED-CODE KAPPA (the staff synthesis) — the SAME pipeline function runs
 *     as a BOUNDED batch (over the finite log, for backfills) AND as an
 *     UNBOUNDED stream (live, event by event). One function, two drivers. This
 *     is the Beam/Flink thesis: batch = a bounded stream. You get Kappa's
 *     single-codebase win WITHOUT pure-Kappa's pain of always recomputing from
 *     the top of history.
 *
 * We build one log (with a LATE event), run Lambda's two layers and watch them
 * drift, then run Kappa's one pipeline in batch and stream mode and watch them
 * agree to the exact same number.
 */

import { log } from "../lib/log.ts";

interface Event { eventTime: number; user: string; id: string } // eventTime in seconds

// The immutable event log — the single source of truth. Note e5: its EVENT time
// (3s) is far behind its ARRIVAL position (it shows up last) → it is LATE / out
// of order. A correct aggregation counts it; a corner-cutting one may drop it.
const LOG: Event[] = [
  { eventTime: 1,  user: "alice", id: "e1" },
  { eventTime: 2,  user: "bob",   id: "e2" },
  { eventTime: 4,  user: "alice", id: "e3" },
  { eventTime: 6,  user: "bob",   id: "e4" },
  { eventTime: 3,  user: "alice", id: "e5" }, // LATE: arrives last, event-time is old
  { eventTime: 8,  user: "alice", id: "e6" },
];

const LATENESS_HORIZON = 2; // speed layer only trusts events within 2s of the max it has seen

type Counts = Map<string, number>;
const fmt = (c: Counts) => [...c].sort().map(([u, n]) => `${u}=${n}`).join(" ");

// ─────────────────────────────────────────────────────────────────────────────
// A) LAMBDA — two codebases for the SAME aggregation. Watch them drift.
// ─────────────────────────────────────────────────────────────────────────────

/** BATCH layer (codebase #1): exact. Scans the whole log; late/out-of-order is fine. */
function batchLayer(logEvents: Event[]): Counts {
  const counts: Counts = new Map();
  for (const e of logEvents) counts.set(e.user, (counts.get(e.user) ?? 0) + 1);
  return counts;
}

/** SPEED layer (codebase #2): approximate. Drops events that fall behind the
 *  watermark (maxEventTimeSeen − horizon) to stay fast — so it under-counts. */
function speedLayer(logEvents: Event[]): Counts {
  const counts: Counts = new Map();
  let maxSeen = -Infinity;
  for (const e of logEvents) {
    maxSeen = Math.max(maxSeen, e.eventTime);
    const watermark = maxSeen - LATENESS_HORIZON;
    if (e.eventTime < watermark) continue; // too late for the fast path → dropped
    counts.set(e.user, (counts.get(e.user) ?? 0) + 1);
  }
  return counts;
}

// ─────────────────────────────────────────────────────────────────────────────
// B & C) KAPPA — ONE pipeline function. Same code for batch AND stream.
// ─────────────────────────────────────────────────────────────────────────────

/** The single Kappa pipeline: a pure fold over events. It has no notion of
 *  "batch" vs "stream" — it just accumulates. Correct by construction: it never
 *  drops late data, so replay = reprocess = truth. */
function pipelineStep(counts: Counts, e: Event): Counts {
  counts.set(e.user, (counts.get(e.user) ?? 0) + 1);
  return counts;
}

/** BOUNDED driver (backfill): feed the whole finite log through the pipeline. */
function runBatch(logEvents: Event[]): Counts {
  let state: Counts = new Map();
  for (const e of logEvents) state = pipelineStep(state, e);
  return state;
}

/** UNBOUNDED driver (live): feed events one at a time as they arrive. Same
 *  pipelineStep — the only difference is who calls it and when. */
function runStream(): { push: (e: Event) => void; state: () => Counts } {
  let state: Counts = new Map();
  return { push: (e) => { state = pipelineStep(state, e); }, state: () => state };
}

function main() {
  log("═══ A) LAMBDA — same aggregation, TWO codebases (batch + speed) ═══");
  const batch = batchLayer(LOG);
  const speed = speedLayer(LOG);
  log(`   BATCH layer (exact, whole log):        ${fmt(batch)}`);
  log(`   SPEED layer (fast, drops late e5):     ${fmt(speed)}`);
  const drift = fmt(batch) !== fmt(speed);
  log(`   serving layer must MERGE these → they ${drift ? "DISAGREE ✗" : "agree"} (alice: ${batch.get("alice")} vs ${speed.get("alice")})`);
  log("   The speed layer dropped late event e5, so it UNDER-COUNTS alice. Two");
  log("   engines, two copies of the logic → the DRIFT TAX. Change the rule in one");
  log("   and forget the other, and your dashboards report different truths forever.");

  log("");
  log("═══ B) KAPPA — ONE codebase; reprocess by REPLAYING the log ═══");
  const fresh = runBatch(LOG);
  const replay = runBatch(LOG); // replay from the top of the immutable log = reprocess
  log(`   fresh run over log:                    ${fmt(fresh)}`);
  log(`   REPLAY the same log through same code:  ${fmt(replay)}`);
  log(`   reproducible from event-time? ${fmt(fresh) === fmt(replay) ? "✓ identical" : "✗ mismatch"} — no second codebase to drift.`);

  log("");
  log("═══ C) UNIFIED-CODE KAPPA — one pipeline, two drivers (batch = bounded stream) ═══");
  const backfill = runBatch(LOG); // bounded driver over the finite log
  const live = runStream();       // unbounded driver, event by event
  for (const e of LOG) live.push(e);
  const liveState = live.state();
  log(`   BATCH mode (bounded, backfill):        ${fmt(backfill)}`);
  log(`   STREAM mode (unbounded, live):         ${fmt(liveState)}`);
  log(`   same pipelineStep both times → ${fmt(backfill) === fmt(liveState) ? "✓ identical results" : "✗ mismatch"}. One function, two drivers.`);

  log("");
  log("Takeaway: Lambda's defining flaw is maintaining the SAME logic in two");
  log("engines (batch + speed) that inevitably DRIFT — name that drift tax the");
  log("moment you draw Lambda. Kappa deletes the batch layer: the replayable log");
  log("is the source of truth and you reprocess by replaying. Pure Kappa's cost is");
  log("that replaying huge history is slow/expensive (mitigate: tiered storage,");
  log("parallel replay, or a bounded-batch run on the same code). The staff answer");
  log("is UNIFIED-CODE Kappa — one pipeline run as a bounded batch for backfills");
  log("and an unbounded stream for live — the single-codebase win without the pain.");
  process.exit(0);
}

main();
