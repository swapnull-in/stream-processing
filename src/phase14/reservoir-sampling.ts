/**
 * Phase 14 — RESERVOIR SAMPLING: a fair sample of a stream you can't store. Run: npm run phase14
 *
 * Fourth probabilistic tool, a different kind of question. Count-Min, HLL and
 * Bloom each return a MEASURE (how often, how many, is it there). None of them
 * can hand you EXAMPLES — "show me 1,000 representative requests from the last
 * minute." Keeping everything is the thing we can't afford; keeping "the first
 * 1,000" or "1% of traffic" is either biased or unbounded.
 *
 *   ALGORITHM R. Keep the first k items. For item number i (i > k), pick a
 *   random slot j in 1..i; if j ≤ k, overwrite slot j. That's it.
 *
 *   Why it's fair: item i gets in with probability k/i. Each later item m then
 *   evicts it with probability 1/m. Multiply through and every item — first or
 *   last — is in the final reservoir with probability exactly k/n, for a stream
 *   length n you never had to know.
 *
 * Memory is O(k), one pass, and the SIZE IS FIXED no matter how hard traffic
 * spikes — the opposite of "keep 1%", whose output grows with the spike.
 *
 * Two things break in a real pipeline, and both are the interview:
 *
 *   • MERGING ACROSS SHARDS. Concatenating per-shard reservoirs and re-sampling
 *     is BIASED: a quiet shard with 1,000 events and a busy one with 99,000
 *     each contribute k, so the quiet shard is wildly over-represented. The fix
 *     is BOTTOM-K: give every item a random key, keep the k SMALLEST keys per
 *     shard, and merge by taking the k smallest overall. That is a uniform
 *     sample of the union, with no coordination — the same mergeability that
 *     makes HLL (phase4) work across shards.
 *
 *   • RARE EVENTS DROWN. If 0.5% of requests are errors, a uniform sample of
 *     200 holds about one. STRATIFY: one reservoir per (endpoint, status), so
 *     every class gets its k examples however rare it is.
 *
 * We demo: (A) a single stream — each third of the stream gets its fair share,
 * while "keep the first k" sees only the start; (B) two unequal shards — naive
 * concatenation vs bottom-k merge; (C) a 0.5% error class, uniform vs stratified.
 *
 * TAKEAWAY: when the ask is examples, not a count, keep a reservoir — fixed
 * size, every item equally likely. Merge with bottom-k keys, never by
 * concatenating; stratify so rare classes aren't drowned out.
 */

import { log } from "../lib/log.ts";

// ── Deterministic PRNG (seeded mulberry32) — no Math.random in the pipeline ──
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ── Algorithm R ──────────────────────────────────────────────────────────────

class Reservoir<T> {
  readonly items: T[] = [];
  seen = 0;
  private readonly k: number;
  private readonly rand: () => number;

  constructor(k: number, rand: () => number) {
    this.k = k;
    this.rand = rand;
  }

  add(item: T): void {
    this.seen++;
    if (this.items.length < this.k) { this.items.push(item); return; }
    const j = Math.floor(this.rand() * this.seen);   // uniform in 0..seen-1
    if (j < this.k) this.items[j] = item;            // replace with probability k/seen
  }
}

// ── Bottom-k: the mergeable version ──────────────────────────────────────────

interface Keyed<T> { key: number; item: T }

class BottomK<T> {
  kept: Keyed<T>[] = [];       // the k items with the smallest random keys seen so far
  private readonly k: number;
  private readonly rand: () => number;

  constructor(k: number, rand: () => number) {
    this.k = k;
    this.rand = rand;
  }

  add(item: T): void {
    this.offer({ key: this.rand(), item });
  }

  private offer(entry: Keyed<T>): void {
    if (this.kept.length < this.k) { this.kept.push(entry); return; }
    // Replace the current largest key if this one is smaller. (A max-heap makes
    // this O(log k); a linear scan keeps the demo readable.)
    let worst = 0;
    for (let i = 1; i < this.kept.length; i++) if (this.kept[i]!.key > this.kept[worst]!.key) worst = i;
    if (entry.key < this.kept[worst]!.key) this.kept[worst] = entry;
  }

  /** Union of two samples = the k smallest keys across both. No coordination needed. */
  static merge<T>(a: BottomK<T>, b: BottomK<T>, k: number): Keyed<T>[] {
    return [...a.kept, ...b.kept].sort((x, y) => x.key - y.key).slice(0, k);
  }
}

const pct = (n: number, d: number) => `${((100 * n) / d).toFixed(1)}%`;

// ── (A) One stream: is every part of it equally likely to be kept? ────────────

log("(A) ONE STREAM of 30,000 events, reservoir of k = 300");
{
  const N = 30_000;
  const K = 300;
  const TRIALS = 200;            // repeat with different seeds so the fairness is visible, not luck
  const thirds = [0, 0, 0];
  for (let t = 0; t < TRIALS; t++) {
    const reservoir = new Reservoir<number>(K, mulberry32(1000 + t));
    for (let i = 0; i < N; i++) reservoir.add(i);
    for (const i of reservoir.items) thirds[Math.floor((3 * i) / N)]!++;
  }
  const total = thirds.reduce((a, b) => a + b, 0);
  log(`   share of the sample from each third of the stream (ideal 33.3% each, ${TRIALS} runs):`);
  log(`   first ${pct(thirds[0]!, total)} · middle ${pct(thirds[1]!, total)} · last ${pct(thirds[2]!, total)}`);
  log(`   memory: ${K} items, whether the stream is 30 thousand or 30 billion`);
  log("   compare 'keep the first 300': 100% from the first third, 0% from the rest — a sample of the warm-up");
}

// ── (B) Two shards of very different size ────────────────────────────────────

log("");
log("(B) TWO SHARDS — quiet (1,000 events) and busy (99,000 events), sample k = 200 of the union");
{
  const K = 200;
  const QUIET = 1_000;
  const BUSY = 99_000;
  const TRIALS = 40;
  let naiveQuiet = 0;
  let bottomQuiet = 0;
  for (let t = 0; t < TRIALS; t++) {
    const rand = mulberry32(5000 + t);

    // Naive: a reservoir per shard, concatenate, re-sample k from the 2k.
    const rq = new Reservoir<string>(K, rand);
    const rb = new Reservoir<string>(K, rand);
    for (let i = 0; i < QUIET; i++) rq.add("quiet");
    for (let i = 0; i < BUSY; i++) rb.add("busy");
    const pool = [...rq.items, ...rb.items];
    const resample = new Reservoir<string>(K, rand);
    for (const item of pool) resample.add(item);
    naiveQuiet += resample.items.filter((s) => s === "quiet").length;

    // Bottom-k: random key per item, keep the smallest k per shard, merge by key.
    const bq = new BottomK<string>(K, rand);
    const bb = new BottomK<string>(K, rand);
    for (let i = 0; i < QUIET; i++) bq.add("quiet");
    for (let i = 0; i < BUSY; i++) bb.add("busy");
    bottomQuiet += BottomK.merge(bq, bb, K).filter((e) => e.item === "quiet").length;
  }
  const denom = K * TRIALS;
  log(`   the quiet shard produced ${pct(QUIET, QUIET + BUSY)} of all events`);
  log(`   naive concat + re-sample: quiet shard is ${pct(naiveQuiet, denom)} of the sample  ❌ ~50× over-represented`);
  log(`   bottom-k merge:           quiet shard is ${pct(bottomQuiet, denom)} of the sample  ✅ matches its true share`);
}

// ── (C) Rare classes: uniform vs stratified ──────────────────────────────────

log("");
log("(C) RARE EVENTS — 0.5% of 40,000 requests are errors; we want examples of both");
{
  const N = 40_000;
  const K = 200;
  const rand = mulberry32(777);
  const uniform = new Reservoir<string>(K, rand);
  const strata = new Map<string, Reservoir<string>>();
  let errors = 0;
  for (let i = 0; i < N; i++) {
    const status = rand() < 0.005 ? "500" : "200";
    if (status === "500") errors++;
    uniform.add(status);
    let stratum = strata.get(status);
    if (!stratum) { stratum = new Reservoir<string>(K / 2, rand); strata.set(status, stratum); }
    stratum.add(status);
  }
  const uniformErrors = uniform.items.filter((s) => s === "500").length;
  log(`   the stream held ${errors} errors out of ${N} requests`);
  log(`   uniform reservoir of ${K}:        ${uniformErrors} error example${uniformErrors === 1 ? "" : "s"} — too few to debug from`);
  log(`   stratified, ${K / 2} per status:     ${strata.get("500")!.items.length} error examples and ${strata.get("200")!.items.length} success examples`);
  log("   keep each stratum's `seen` count alongside it, so estimates can be re-weighted back to true proportions");
}

log("");
log("TAKEAWAY: fixed-size and fair by construction; merge with bottom-k keys, never by concatenating; stratify the rare.");
