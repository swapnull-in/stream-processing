/**
 * Phase 13 — CAPSTONE: TOP-K MUSIC ANALYTICS (the "Spotify Wrapped" problem).
 * Run: node "src/phase13/topk-analytics.ts"
 *
 * The classic staff-level prompt: "report the top most-listened songs — hourly,
 * daily, by country." Name the problem class first: this is TOP-K HEAVY HITTERS
 * over a (dimension × window) cube — a COUNTING problem plus a RANKING problem.
 * Once you name it, the architecture is a menu with two priced paths:
 *
 *   FAST PATH (approximate, seconds–minutes) — the "trending now" surface.
 *     Per shard, per tumbling window (Phase 1): a Count-Min Sketch (Phase 3)
 *     counts every play in FIXED memory, and a small MIN-HEAP of K candidates
 *     rides alongside it. The heap ranks by the sketch's ESTIMATED counts —
 *     it has to, the estimate is the only number the fast path ever has. CMS
 *     only ever OVER-counts, so a tail song can occasionally be inflated into
 *     the heap — that's the ε you bought. Sketches are counter grids, so
 *     MERGING two is element-wise addition: merge across shards for the
 *     cluster-wide window, merge windows for the day. Hours sum into days —
 *     roll up, never recount.
 *
 *   SLOW PATH (exact, hourly/nightly) — the system of record.
 *     A batch count over the full immutable event log: one hashmap entry per
 *     distinct song, memory grows with the catalog (70M songs in production),
 *     but the numbers are TRUE. The batch corrects the stream's drift — the
 *     trending widget tolerates ε; the artist-payout report never does.
 *
 * One event stream, two aggregation contracts. Events are sharded by USER
 * (never by song — a celebrity drop would make one song a write hot key);
 * the real pipeline also dedups by event_id and allocates by event time
 * (Phase 2), which we take as given here to keep the lens on top-K.
 *
 * We stream ~50k plays over a ~2k-song zipf catalog through both paths and
 * compare: hourly fast-path charts, a two-shard sketch merge verified against
 * the global sketch, fast vs exact daily top-10, and the memory bill.
 */

import { hash32, log } from "../lib/log.ts";

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

// ── Count-Min Sketch (Phase 3's structure, plus MERGE) ──────────────────────
// Merging works because the grid is pure counters and every sketch uses the
// same (d, w) and the same hash seeds: sum the cells and you get exactly the
// sketch you'd have built from the concatenated streams.
class CountMinSketch {
  private d: number;
  private w: number;
  private grid: Uint32Array[];
  constructor(d: number, w: number) {
    this.d = d;
    this.w = w;
    this.grid = Array.from({ length: d }, () => new Uint32Array(w));
  }
  add(item: string, count = 1): void {
    for (let i = 0; i < this.d; i++) this.grid[i][hash32(item, i) % this.w] += count;
  }
  estimate(item: string): number {
    let min = Infinity;
    for (let i = 0; i < this.d; i++) min = Math.min(min, this.grid[i][hash32(item, i) % this.w]);
    return min;
  }
  merge(other: CountMinSketch): void {
    for (let i = 0; i < this.d; i++)
      for (let j = 0; j < this.w; j++) this.grid[i][j] += other.grid[i][j];
  }
  /** Cell-by-cell equality — used to prove shard-merge == global sketch. */
  equals(other: CountMinSketch): boolean {
    for (let i = 0; i < this.d; i++)
      for (let j = 0; j < this.w; j++) if (this.grid[i][j] !== other.grid[i][j]) return false;
    return true;
  }
  bytes(): number { return this.d * this.w * 4; }
}

// ── Min-heap of K candidates, keyed by ESTIMATED count ──────────────────────
// The fast path's ranking half. After each sketch update we offer the song with
// its fresh estimate: if it beats the heap's minimum, the weakest candidate is
// evicted. Estimates only grow within a window, so an in-heap update sifts down.
class TopKHeap {
  private k: number;
  private heap: { song: string; est: number }[];
  private pos: Map<string, number>;
  constructor(k: number) {
    this.k = k;
    this.heap = [];
    this.pos = new Map();
  }
  offer(song: string, est: number): void {
    const at = this.pos.get(song);
    if (at !== undefined) { this.heap[at].est = est; this.siftDown(at); return; }
    if (this.heap.length < this.k) {
      this.heap.push({ song, est });
      this.pos.set(song, this.heap.length - 1);
      this.siftUp(this.heap.length - 1);
    } else if (est > this.heap[0].est) {
      this.pos.delete(this.heap[0].song);
      this.heap[0] = { song, est };
      this.pos.set(song, 0);
      this.siftDown(0);
    }
  }
  candidates(): string[] { return this.heap.map((e) => e.song); }
  bytes(): number { return this.k * 32; } // ~k small records — a rounding error
  private swap(i: number, j: number): void {
    [this.heap[i], this.heap[j]] = [this.heap[j], this.heap[i]];
    this.pos.set(this.heap[i].song, i);
    this.pos.set(this.heap[j].song, j);
  }
  private siftUp(i: number): void {
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (this.heap[p].est <= this.heap[i].est) break;
      this.swap(i, p); i = p;
    }
  }
  private siftDown(i: number): void {
    for (;;) {
      const l = 2 * i + 1, r = 2 * i + 2;
      let s = i;
      if (l < this.heap.length && this.heap[l].est < this.heap[s].est) s = l;
      if (r < this.heap.length && this.heap[r].est < this.heap[s].est) s = r;
      if (s === i) break;
      this.swap(i, s); i = s;
    }
  }
}

// ── The event stream: ~50k plays, zipf-ish over ~2k songs, 3 hours ──────────
interface Play { ts: number; user: string; song: string }

function buildStream(nPlays: number, nSongs: number, nUsers: number): Play[] {
  // Zipf weights: song rank r gets weight 1/r — a few megahits, a long tail.
  const cum = new Float64Array(nSongs);
  let total = 0;
  for (let r = 0; r < nSongs; r++) { total += 1 / (r + 1); cum[r] = total; }
  const rand = mulberry32(20260816);
  const plays: Play[] = [];
  for (let i = 0; i < nPlays; i++) {
    const x = rand() * total; // binary-search the cumulative weights
    let lo = 0, hi = nSongs - 1;
    while (lo < hi) { const mid = (lo + hi) >> 1; if (cum[mid] < x) lo = mid + 1; else hi = mid; }
    plays.push({
      ts: Math.floor(rand() * 3 * 3600), // event time within a 3-hour day
      user: `user-${Math.floor(rand() * nUsers)}`,
      song: `song-${String(lo + 1).padStart(4, "0")}`,
    });
  }
  return plays;
}

function main() {
  const K = 10, D = 4, W = 1024, SHARDS = 2, WINDOW_SEC = 3600;
  const plays = buildStream(50_000, 2_000, 5_000);
  const distinct = new Set(plays.map((p) => p.song)).size;
  log(`═══ Stream: ${plays.length.toLocaleString()} plays, ${distinct.toLocaleString()} distinct songs, 3 hourly windows, ${SHARDS} shards ═══`);
  log("");

  // ── FAST PATH: per (shard, hourly window) → CMS + top-K heap ──────────────
  // Shard by USER (hot-song-proof), window by tumbling hour (Phase 1).
  const sketches = new Map<string, CountMinSketch>(); // "shard:window" → CMS
  const heaps = new Map<string, TopKHeap>();
  for (const p of plays) {
    const shard = hash32(p.user, 99) % SHARDS;
    const win = Math.floor(p.ts / WINDOW_SEC) * WINDOW_SEC;
    const key = `${shard}:${win}`;
    let cms = sketches.get(key);
    if (!cms) { cms = new CountMinSketch(D, W); sketches.set(key, cms); }
    let heap = heaps.get(key);
    if (!heap) { heap = new TopKHeap(K); heaps.set(key, heap); }
    cms.add(p.song);
    heap.offer(p.song, cms.estimate(p.song)); // rank by the ESTIMATE — it's all we have
  }

  // Per hourly window: merge the shards' sketches, pool their candidates,
  // re-estimate on the merged sketch, rank. (Flink's merge step, in miniature.)
  log("═══ FAST PATH — hourly top-3 per window (merged across shards) ═══");
  const windows = [0, 3600, 7200];
  const daily = new CountMinSketch(D, W);
  const dailyCandidates = new Set<string>();
  for (const win of windows) {
    const merged = new CountMinSketch(D, W);
    const pool = new Set<string>();
    for (let s = 0; s < SHARDS; s++) {
      const key = `${s}:${win}`;
      merged.merge(sketches.get(key)!);
      for (const c of heaps.get(key)!.candidates()) pool.add(c);
    }
    daily.merge(merged); // the day is the SUM of its hours — rollup, never recount
    for (const c of pool) dailyCandidates.add(c);
    const top3 = [...pool]
      .map((song) => ({ song, est: merged.estimate(song) }))
      .sort((a, b) => b.est - a.est || (a.song < b.song ? -1 : 1))
      .slice(0, 3);
    const h = win / 3600;
    log(`   hour [${h}:00–${h + 1}:00): ${top3.map((t) => `${t.song}≈${t.est}`).join("  ")}`);
  }
  log("");

  // ── Prove the merge: shard sketches summed == one global sketch ───────────
  const global = new CountMinSketch(D, W);
  for (const p of plays) global.add(p.song);
  const allMerged = new CountMinSketch(D, W);
  for (const cms of sketches.values()) allMerged.merge(cms);
  log("═══ Sketch mergeability check ═══");
  log(`   Σ(2 shards × 3 windows) grid == global single-sketch grid, cell for cell: ${allMerged.equals(global)}`);
  log("   counters just add — shards and windows merge for free. This is WHY the");
  log("   fast path distributes: no shard needs to see the whole stream.");
  log("");

  // ── SLOW PATH: exact batch count over the full immutable log ──────────────
  const exact = new Map<string, number>();
  for (const p of plays) exact.set(p.song, (exact.get(p.song) ?? 0) + 1);
  const exactTop = [...exact.entries()]
    .sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1))
    .slice(0, K);

  // Daily fast-path chart: pooled hourly candidates ranked on the daily sketch.
  const fastTop = [...dailyCandidates]
    .map((song) => ({ song, est: daily.estimate(song) }))
    .sort((a, b) => b.est - a.est || (a.song < b.song ? -1 : 1))
    .slice(0, K);

  log("═══ DAILY TOP-10 — fast path (sketch estimate) vs slow path (exact batch) ═══");
  log("   rank  fast path            est     exact batch          true    match");
  const exactSet = new Set(exactTop.map(([s]) => s));
  let overlap = 0;
  for (let i = 0; i < K; i++) {
    const f = fastTop[i], [eSong, eCount] = exactTop[i];
    const hit = exactSet.has(f.song);
    if (hit) overlap++;
    log(`   ${String(i + 1).padStart(3)}   ${f.song.padEnd(12)} ${String(f.est).padStart(10)}     ${eSong.padEnd(12)} ${String(eCount).padStart(9)}    ${hit ? "✓" : "✗ (ε!)"}`);
  }
  log(`   precision: ${overlap}/${K} of the fast-path chart are in the true top-${K}.`);
  log("   CMS never under-counts, so estimates run a touch HIGH — good enough for");
  log("   trending, and the nightly exact batch corrects whatever ε let slip in.");
  log("");

  // ── The memory bill ────────────────────────────────────────────────────────
  const sketchBytes = [...sketches.values()].reduce((n, c) => n + c.bytes(), 0);
  const heapBytes = [...heaps.values()].reduce((n, h) => n + h.bytes(), 0);
  const mapBytes = [...exact.keys()].reduce((n, k) => n + 2 * k.length + 48, 0); // ~string + number + entry overhead
  log("═══ Memory: fixed sketches vs a hashmap that grows with the catalog ═══");
  log(`   fast path: ${sketches.size} sketches (${D}×${W}) + heaps  = ${(sketchBytes + heapBytes).toLocaleString()} bytes — FIXED, whatever the catalog`);
  log(`   slow path: hashmap of ${exact.size.toLocaleString()} songs        ≈ ${mapBytes.toLocaleString()} bytes — and it scales LINEARLY:`);
  log(`   at Spotify's ~70M-song catalog that map is ~${Math.round((70_000_000 * (mapBytes / exact.size)) / 1e9)} GB per window per node;`);
  log(`   the sketches would still be ${(sketchBytes + heapBytes).toLocaleString()} bytes.`);
  log("");

  log("TAKEAWAY: top-K at scale is a counting cube plus a ranking, and top-K is");
  log("cheap once the counting is right. Exact and fresh are both features with");
  log("price tags: the trending surface buys APPROXIMATE — windowed CMS + a heap");
  log("ranked on estimates, fixed memory, sketches that merge across shards and");
  log("roll up across windows (hours sum into days — never recount) — while the");
  log("batch over the immutable log stays the system of record and corrects the");
  log("stream. One event stream, two aggregation contracts: trending tolerates ε;");
  log("the payout report tolerates hours, never estimation error.");
  process.exit(0);
}

main();
