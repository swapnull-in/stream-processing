/**
 * Phase 10 — BACKPRESSURE: the slow downstream that saves your pipeline. Run: node src/phase10/backpressure.ts
 *
 * Backpressure sounds like a bug ("the pipeline is stalling!"). It is a FEATURE.
 * It is the mechanism by which a SLOW downstream operator signals upstream — and
 * ultimately the source — to slow down, so records never pile into unbounded
 * memory and OOM the process. The system trades LATENCY for STABILITY: it would
 * rather run behind than lose data or fall over.
 *
 * The pipeline:  SOURCE → MAP → WINDOW → SINK
 * Each operator has a BOUNDED input buffer (capacity CAP) and a per-tick
 * processing capacity (RATE). The SINK is slow — the OLAP store it writes to is
 * busy and drains only SINK_SLOW records/tick while RATE arrive. That single slow
 * spot is the whole story.
 *
 *   A) CREDIT-BASED FLOW CONTROL — a downstream operator grants "credits" upstream
 *      equal to the free space in its input buffer. Upstream may only push as many
 *      records as it has credits. When the SINK's buffer fills it withholds credits
 *      from WINDOW, which fills and withholds from MAP, which fills and withholds
 *      from SOURCE, which then reads Kafka more slowly. Throttle CASCADES BACKWARD:
 *      sink → window → map → source, one hop per tick.
 *
 *   B) KAFKA LAG GROWS SAFELY — because the SOURCE slows its reads, unconsumed
 *      records don't pile into memory: they sit in the durable Kafka log as
 *      CONSUMER LAG. Operator memory stays BOUNDED (≤ 3·CAP forever); the system
 *      degrades to higher latency, no data loss. A NAIVE consumer with no flow
 *      control reads greedily into an in-memory buffer that grows without bound → OOM.
 *
 *   C) LOCATE THE BOTTLENECK — backpressure is a SYMPTOM; the fix is the slow
 *      operator, and it differs by WHERE the pressure originates. The origin is the
 *      most-downstream operator whose buffer is full while its own downstream is not.
 *      Full sink → the store is I/O-bound → scale/batch the sink. Full map with an
 *      un-full window → the map function is CPU-bound → a different fix entirely.
 *
 *   D) RECOVERY — when the sink speeds up (the burst passes), credits flow again,
 *      the source resumes full-rate reads, and the accumulated Kafka lag drains.
 *
 * Deterministic virtual time: one integer TICK. No Date.now, no Math.random.
 */

import { log } from "../lib/log.ts";

const CAP = 6;          // input-buffer capacity of every operator (records)
const RATE = 6;         // per-tick processing capacity of source/map/window (and the fast sink)
const SINK_SLOW = 1;    // busy OLAP store: drains only 1 record/tick
const SINK_FAST = 6;    // burst passed: store drains at full rate again
const PRODUCE = 3;      // Kafka producer appends this many records/tick to the durable log
const SLOW_TICKS = 12;  // ticks the sink stays slow
const MAX_TICKS = 19;   // stop the recovery loop by here
const MEM_MAX = 3 * CAP; // hard ceiling on total operator memory (bounded by design)

/** Render a bounded buffer as a fill bar, e.g. ████·· , with a FULL tag. */
function bar(buf: number): string {
  return "█".repeat(buf) + "·".repeat(CAP - buf) + (buf >= CAP ? " FULL" : "     ");
}

interface State { mapBuf: number; winBuf: number; sinkBuf: number; kafkaLog: number; consumed: number; written: number }

// One tick of the CREDIT-BASED pipeline. Process downstream→upstream so freed
// space becomes credit the same tick. credit(op) = CAP − op.buf (free space).
function tick(s: State, sinkRate: number): { read: number; drained: number; throttled: boolean } {
  s.kafkaLog += PRODUCE; // producer keeps appending to the durable log regardless

  // SINK drains to the OLAP store (its "downstream" is the store, not a buffer).
  const drained = Math.min(s.sinkBuf, sinkRate);
  s.sinkBuf -= drained;
  s.written += drained;

  // WINDOW → SINK, limited by the credit the sink grants (its free space).
  const sinkCredit = CAP - s.sinkBuf;
  const wMove = Math.min(RATE, s.winBuf, sinkCredit);
  s.winBuf -= wMove; s.sinkBuf += wMove;

  // MAP → WINDOW, limited by the window's credit.
  const winCredit = CAP - s.winBuf;
  const mMove = Math.min(RATE, s.mapBuf, winCredit);
  s.mapBuf -= mMove; s.winBuf += mMove;

  // SOURCE reads Kafka → MAP, limited by the map's credit. This is where
  // backpressure reaches the source: fewer credits ⇒ fewer reads ⇒ lag grows.
  const avail = s.kafkaLog - s.consumed;      // records waiting in the durable log
  const mapCredit = CAP - s.mapBuf;
  const read = Math.min(RATE, avail, mapCredit);
  s.consumed += read; s.mapBuf += read;

  // The source is BACKPRESSURED (not merely out of data) when credit is the limit.
  const throttled = mapCredit < Math.min(RATE, avail);
  return { read, drained, throttled };
}

function main() {
  const s: State = { mapBuf: 0, winBuf: 0, sinkBuf: 0, kafkaLog: 0, consumed: 0, written: 0 };
  const wasFull = { SINK: false, WINDOW: false, MAP: false };
  let sourceThrottledLogged = false;

  // ── A) CREDIT-BASED FLOW CONTROL: watch the throttle cascade backward ──────
  log("═══ A) BACKPRESSURE CASCADES BACKWARD (sink → window → map → source) ═══");
  log("   SINK is slow (OLAP store busy, drains 1/tick). Each full buffer withholds");
  log("   credits from the operator upstream. mem = map+window+sink buffers.");
  log("");
  for (let t = 1; t <= SLOW_TICKS; t++) {
    const { read, drained, throttled } = tick(s, SINK_SLOW);
    const mem = s.mapBuf + s.winBuf + s.sinkBuf;
    const lag = s.kafkaLog - s.consumed;
    log(`   t=${String(t).padStart(2)}s  read=${read}/${RATE}  MAP[${bar(s.mapBuf)}] WIN[${bar(s.winBuf)}] SINK[${bar(s.sinkBuf)}]  drain=${drained}  lag=${lag}  mem=${mem}`);

    // Announce the cascade the first time each buffer fills, downstream→upstream.
    if (s.sinkBuf >= CAP && !wasFull.SINK) { wasFull.SINK = true; log(`        ↑ SINK buffer FULL → withholds credits from WINDOW (throttles it)`); }
    if (s.winBuf >= CAP && !wasFull.WINDOW) { wasFull.WINDOW = true; log(`        ↑ WINDOW buffer FULL → withholds credits from MAP (throttles it)`); }
    if (s.mapBuf >= CAP && !wasFull.MAP) { wasFull.MAP = true; log(`        ↑ MAP buffer FULL → withholds credits from SOURCE (throttles it)`); }
    if (throttled && !sourceThrottledLogged) { sourceThrottledLogged = true; log(`        ↑ SOURCE now throttled → reads Kafka slower → unconsumed records become LAG`); }
  }

  const lagAfterSlow = s.kafkaLog - s.consumed;
  const memAfterSlow = s.mapBuf + s.winBuf + s.sinkBuf;
  log("");
  log(`   After ${SLOW_TICKS} slow ticks: Kafka LAG = ${lagAfterSlow} records, operator MEM = ${memAfterSlow} (ceiling ${MEM_MAX}).`);
  log("   The backlog lives in the durable log, NOT in memory. Latency degraded; no data lost.");

  // ── B) LAG GROWS SAFELY vs a NAIVE consumer that OOMs ─────────────────────
  log("");
  log("═══ B) DEGRADE LATENCY, NOT DATA — bounded memory vs a naive consumer ═══");
  log("   The credit pipeline held memory FLAT while lag absorbed the overload.");
  log("   A NAIVE consumer has no flow control: it reads greedily into an in-memory");
  log("   buffer and drains at the same slow sink rate. Watch it climb to OOM.");
  log("");
  const OOM = 30;
  let nMem = 0, nKafka = 0, nRead = 0;
  for (let t = 1; t <= 15; t++) {
    nKafka += PRODUCE;
    const avail = nKafka - nRead;
    const rd = Math.min(RATE, avail);   // no credit check — read everything it can
    nRead += rd; nMem += rd;
    const out = Math.min(nMem, SINK_SLOW);
    nMem -= out;
    const bounded = Math.min(memAfterSlow, MEM_MAX);
    log(`   t=${String(t).padStart(2)}s  naiveMem=${String(nMem).padStart(2)}  (credit-pipeline mem stays ${bounded}, ceiling ${MEM_MAX})`);
    if (nMem >= OOM) { log(`        ✗ naiveMem ${nMem} ≥ OOM limit ${OOM} → OUT OF MEMORY, consumer crashes, in-flight data LOST`); break; }
  }
  log("");
  log("   Credit pipeline: memory BOUNDED, backlog safe in Kafka. Naive: memory UNBOUNDED → OOM.");

  // ── C) LOCATE THE BOTTLENECK — the staff diagnostic ───────────────────────
  log("");
  log("═══ C) LOCATE THE BOTTLENECK (backpressure is the symptom, not the cause) ═══");
  // Origin = most-downstream operator that is FULL while its own downstream is not.
  const chain = [
    { n: "SINK",   buf: s.sinkBuf, downFull: false /* downstream is the external store */,
      fix: "I/O-BOUND: the OLAP store drains slower than records arrive. FIX → scale the sink (more writer threads / partitions) or BATCH writes. Do NOT 'turn off backpressure' — you'd just move the OOM into memory." },
    { n: "WINDOW", buf: s.winBuf, downFull: s.sinkBuf >= CAP,
      fix: "STATE-BOUND: window aggregation can't keep up. FIX → shard the window state / add parallelism." },
    { n: "MAP",    buf: s.mapBuf, downFull: s.winBuf >= CAP,
      fix: "CPU-BOUND: the map function is expensive. FIX → optimise it or parallelise across more partitions. A DIFFERENT fix than a slow sink." },
  ];
  const fullStages = chain.filter((c) => c.buf >= CAP).map((c) => c.n).join(", ");
  const origin = chain.find((c) => c.buf >= CAP && !c.downFull);
  log(`   Buffers currently FULL: ${fullStages || "none"}.`);
  log(`   Origin = the deepest full operator whose downstream is NOT full → ${origin ? origin.n : "?"}.`);
  log(`   Diagnosis: ${origin ? origin.fix : "no backpressure"}`);
  log("   (Had MAP been full while WINDOW was not, the origin would be MAP — CPU-bound — a different fix.)");

  // ── D) RECOVERY — the burst passes, credits flow, lag drains ──────────────
  log("");
  log("═══ D) RECOVERY — sink speeds up, credits flow, Kafka lag drains ═══");
  log("");
  for (let t = SLOW_TICKS + 1; t <= MAX_TICKS; t++) {
    const { read, drained } = tick(s, SINK_FAST);
    const mem = s.mapBuf + s.winBuf + s.sinkBuf;
    const lag = s.kafkaLog - s.consumed;
    log(`   t=${String(t).padStart(2)}s  read=${read}/${RATE}  MAP[${bar(s.mapBuf)}] WIN[${bar(s.winBuf)}] SINK[${bar(s.sinkBuf)}]  drain=${drained}  lag=${lag}  mem=${mem}`);
    if (lag === 0) { log(`        ✓ SINK fast again → source reads ${read}/tick > produce ${PRODUCE}/tick → lag fully drained, pipeline caught up`); break; }
  }

  log("");
  log("Backpressure is a FEATURE. A slow sink withholds credits that cascade upstream");
  log("(sink → window → map → source), so the source reads Kafka slower and the overload");
  log("accumulates as durable KAFKA LAG instead of OOMing operator memory — the pipeline");
  log("degrades to higher latency, never to data loss. A naive consumer with no flow");
  log("control OOMs instead. Diagnose by WHERE it originates: a slow sink means scale or");
  log("batch the sink; a slow map means it's CPU-bound — the fix differs by location.");
  log("Backpressure is the symptom; the slow operator is the cause.");
  process.exit(0);
}

main();
