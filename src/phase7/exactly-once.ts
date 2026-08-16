/**
 * Phase 7 — CHECKPOINTING & EXACTLY-ONCE. Run: npm run phase7
 *
 * The question interviewers probe hardest, and the one most people get wrong.
 * "Exactly-once" does NOT mean each message is DELIVERED once. Networks and
 * crashes make that impossible — you always retry, so wires see duplicates.
 * What you actually guarantee is a property of the EFFECT ON STATE: after a
 * crash and replay, the state reads as if every event was applied exactly once.
 * The honest name is "effectively-once." And it is only ever as strong as the
 * SINK you write to.
 *
 * The machine:
 *   • A SOURCE with replayable offsets — an array + an offset pointer. Replay =
 *     rewind the pointer and re-read. (Kafka's log; a WAL.)
 *   • An OPERATOR with keyed state — a per-key running count (Map<key,count>).
 *   • A periodic CHECKPOINT that atomically snapshots {operatorState, offset}
 *     to a durable store. This is the Chandy-Lamport idea: capture the state AND
 *     the input position TOGETHER, so they agree. (In a real DAG a "barrier"
 *     flows downstream through every operator; each snapshots when the barrier
 *     passes, giving a globally consistent cut. We model one operator.)
 *   • A controllable CRASH. On recovery we restore {state, offset} from the last
 *     checkpoint and REPLAY the source from that offset.
 *
 * The subtlety: the operator's OWN state rolls back cleanly with the checkpoint,
 * so it always recovers exactly-once. The SINK is EXTERNAL — its side effects
 * already happened and cannot be un-done. Replay re-emits the events between the
 * last checkpoint and the crash. Whether that corrupts the result depends
 * entirely on the sink:
 *
 *   A) NON-IDEMPOTENT sink (`counter += 1`) → those events land TWICE → over-count.
 *   B) IDEMPOTENT UPSERT keyed by the event's unique id (`sink[id] = 1`) → the
 *      re-emit OVERWRITES instead of adding → the total CONVERGES to the truth.
 *
 * We run the same crash + replay against both sinks and watch one break.
 */

import { log } from "../lib/log.ts";

interface Event { offset: number; id: string; key: string }

// The replayable source: 10 page-view events, tagged by country (the state key).
// Truth: US=5, IN=3, UK=2, total = 10.
const STREAM: Event[] = [
  { offset: 0, id: "ev0", key: "US" },
  { offset: 1, id: "ev1", key: "IN" },
  { offset: 2, id: "ev2", key: "US" },
  { offset: 3, id: "ev3", key: "UK" }, // ← checkpoint fires after this (offset 4)
  { offset: 4, id: "ev4", key: "US" },
  { offset: 5, id: "ev5", key: "IN" },
  { offset: 6, id: "ev6", key: "US" }, // ← CRASH after this, BEFORE the next checkpoint
  { offset: 7, id: "ev7", key: "UK" },
  { offset: 8, id: "ev8", key: "US" }, // ← checkpoint fires after this (offset 8)
  { offset: 9, id: "ev9", key: "IN" },
];

const CHECKPOINT_EVERY = 4; // snapshot {state, offset} every 4 processed events
const CRASH_AFTER = 6;      // crash right after processing offset 6 (uncheckpointed 4,5,6)
const TRUTH = STREAM.length; // one increment per event = 10

type SinkKind = "counter" | "upsert";
interface Checkpoint { offset: number; state: Map<string, number> }

/**
 * Run the source→operator→sink pipeline once, optionally crashing mid-stream.
 * The checkpoint snapshots {state, offset} together; on crash we restore both
 * and replay. The external sink is NOT rolled back — its effects already shipped.
 */
function simulate(sinkKind: SinkKind, crashAfter: number | null): { sinkTotal: number; state: Map<string, number> } {
  // Durable store — survives the crash. Starts empty at offset 0.
  let checkpoint: Checkpoint = { offset: 0, state: new Map() };

  // Volatile operator state + source position — lost on crash, restored from checkpoint.
  let state = new Map<string, number>();
  let offset = checkpoint.offset;

  // The EXTERNAL sink. Persisted downstream; a crash cannot un-write it.
  let counterSink = 0;               // A) non-idempotent: blind += per event
  const upsertSink = new Map<string, number>(); // B) idempotent: id → 1 (overwrite)

  let crashed = false;

  while (offset < STREAM.length) {
    const e = STREAM[offset];

    // ── Operator applies the event to keyed state ──
    state.set(e.key, (state.get(e.key) ?? 0) + 1);

    // ── Emit the effect to the sink ──
    if (sinkKind === "counter") { counterSink += 1; }
    else { upsertSink.set(e.id, 1); } // deterministic key = event id → idempotent

    log(`      · processed offset ${offset} (${e.id}, ${e.key})`);
    offset += 1;

    // ── Periodic checkpoint: snapshot state AND offset ATOMICALLY ──
    if (offset % CHECKPOINT_EVERY === 0) {
      checkpoint = { offset, state: new Map(state) };
      log(`   ⟶ CHECKPOINT: {offset=${checkpoint.offset}, state=${fmt(checkpoint.state)}} → durable store`);
    }

    // ── Controllable crash (fires once) ──
    if (crashAfter !== null && !crashed && e.offset === crashAfter) {
      crashed = true;
      log(`   ✗ CRASH after offset ${e.offset}! Volatile state lost. Sink already shipped its effects (can't rewind them).`);
      // Recover: restore operator state + source offset from the last checkpoint.
      state = new Map(checkpoint.state);
      offset = checkpoint.offset;
      log(`   ↺ RECOVER: restore {offset=${offset}, state=${fmt(state)}} from checkpoint → REPLAY from offset ${offset}`);
    }
  }

  const sinkTotal = sinkKind === "counter"
    ? counterSink
    : [...upsertSink.values()].reduce((a, b) => a + b, 0);
  return { sinkTotal, state };
}

/** Compact "US=5,IN=3" rendering of a keyed-count map, in insertion order. */
function fmt(m: Map<string, number>): string {
  return `{${[...m.entries()].map(([k, v]) => `${k}=${v}`).join(",")}}` || "{}";
}

function main() {
  log("═══ Baseline: no crash — both sinks are correct ═══");
  const base = simulate("counter", null);
  log(`   Baseline total = ${base.sinkTotal} (truth = ${TRUTH}), operator state = ${fmt(base.state)}`);
  log("");

  log("═══ A) AT-LEAST-ONCE + NON-IDEMPOTENT SINK (counter += 1) ═══");
  const a = simulate("counter", CRASH_AFTER);
  log(`   Sink total = ${a.sinkTotal}  ✗ WRONG (truth = ${TRUTH})`);
  log(`   Offsets 4,5,6 were processed, then replayed after recovery → counted TWICE (+3 over).`);
  log(`   Note the OPERATOR state recovered exactly: ${fmt(a.state)} — it rolled back with the checkpoint.`);
  log(`   Only the external non-idempotent sink got corrupted.`);
  log("");

  log("═══ B) EXACTLY-ONCE-BY-EFFECT via IDEMPOTENT UPSERT (sink[eventId] = 1) ═══");
  const b = simulate("upsert", CRASH_AFTER);
  log(`   Sink total = ${b.sinkTotal}  ✓ CORRECT (truth = ${TRUTH}, matches baseline ${base.sinkTotal})`);
  log(`   Same crash, same replay of 4,5,6 — but re-writing sink[ev4],sink[ev5],sink[ev6] OVERWRITES.`);
  log(`   Reprocessing is a no-op → the total CONVERGES. "Effectively-once."`);
  log("");

  log("═══ C) CHECKPOINT + OFFSET RECOVERY (Chandy-Lamport) ═══");
  log("   The checkpoint snapshotted {state, offset} together, so on recovery the");
  log("   restored counts and the restored read-position AGREE — no gap, no overlap");
  log("   in the OPERATOR's own state. In a real DAG a barrier flows through every");
  log("   operator so the whole graph snapshots a single consistent cut.");
  log("");

  log("═══ D) THE SINK IS THE LIMIT ═══");
  log("   You CANNOT get exactly-once with a non-idempotent, non-transactional sink");
  log("   (a plain counter, a blind HTTP POST). Replay after a crash double-applies.");
  log("   End-to-end you need EITHER an idempotent upsert keyed by a deterministic id,");
  log("   OR a transactional / two-phase-commit sink that ties the write to the offset.");
  log("");
  log(`   double-count (non-idempotent) = ${a.sinkTotal}   vs   converged (idempotent) = ${b.sinkTotal}`);
  log("");
  log("   TAKEAWAY: exactly-once is a property of the EFFECT ON STATE, not delivery —");
  log("   \"effectively-once.\" A crash+replay double-counts a non-idempotent sink, but an");
  log("   idempotent upsert keyed by a deterministic key converges to the correct value.");
  log("   Checkpoint state + offset together (Chandy-Lamport) so recovery is consistent;");
  log("   the end-to-end guarantee is only as strong as the sink. Default to at-least-once");
  log("   + idempotent upserts (cheap, rescales freely); reserve 2PC for money movement.");
  process.exit(0);
}

main();
