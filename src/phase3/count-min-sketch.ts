/**
 * Phase 3 — COUNT-MIN SKETCH: counting frequencies in tiny memory. Run: npm run phase3
 *
 * "How many times did each hashtag appear?" over billions of events won't fit in
 * a hashmap — millions of distinct keys is too much RAM. The Count-Min Sketch
 * answers "roughly how many times did X appear?" in FIXED memory, no matter how
 * many distinct items:
 *
 *   A grid of d rows × w counters. To ADD item x: hash it with d independent
 *   hashes and increment one counter per row. To ESTIMATE x: take the MINIMUM of
 *   those d counters. Different items sometimes collide and inflate a counter,
 *   but taking the MIN across rows cancels most collisions — so the sketch only
 *   ever OVER-counts, never under, and the error is bounded and tunable by d, w.
 *
 * It's the engine behind "top-K trending" (sketch + a small heap of candidates).
 * We stream a skewed dataset, estimate frequencies in a fixed 5×2000 grid, and
 * compare to the exact counts.
 */

import { hash32, log } from "../lib/log.ts";

class CountMinSketch {
  private grid: Uint32Array[];
  private d: number;
  private w: number;
  constructor(d = 5, w = 2000) {
    this.d = d; this.w = w;
    this.grid = Array.from({ length: d }, () => new Uint32Array(w));
  }
  add(item: string, count = 1) {
    for (let i = 0; i < this.d; i++) this.grid[i][hash32(item, i) % this.w] += count;
  }
  estimate(item: string): number {
    let min = Infinity;
    for (let i = 0; i < this.d; i++) min = Math.min(min, this.grid[i][hash32(item, i) % this.w]);
    return min;
  }
  bytes() { return this.d * this.w * 4; }
}

function main() {
  // Build a skewed stream: a few hot items, a long tail of rare ones.
  const stream: string[] = [];
  const hot: Record<string, number> = { "#worldcup": 50000, "#election": 30000, "#nba": 12000, "#taylorswift": 8000 };
  for (const [tag, n] of Object.entries(hot)) for (let i = 0; i < n; i++) stream.push(tag);
  for (let i = 0; i < 200000; i++) stream.push(`#tail${i % 50000}`); // 50k rare distinct tags
  // shuffle deterministically
  for (let i = stream.length - 1; i > 0; i--) { const j = (i * 2654435761) % (i + 1); [stream[i], stream[j]] = [stream[j], stream[i]]; }

  const cms = new CountMinSketch(5, 2000);
  const exact = new Map<string, number>();
  for (const item of stream) { cms.add(item); exact.set(item, (exact.get(item) ?? 0) + 1); }

  log(`═══ Streamed ${stream.length.toLocaleString()} events, ${exact.size.toLocaleString()} distinct tags ═══`);
  log(`   exact hashmap would hold ${exact.size.toLocaleString()} entries;`);
  log(`   Count-Min Sketch uses a fixed ${cms.bytes().toLocaleString()} bytes (5×2000 counters) regardless.`);
  log("");
  log("═══ Estimated vs exact frequency for the hot tags ═══");
  log("   tag              exact     estimate   error");
  for (const tag of Object.keys(hot)) {
    const e = exact.get(tag)!, est = cms.estimate(tag);
    log(`   ${tag.padEnd(15)} ${String(e).padStart(6)}    ${String(est).padStart(7)}   +${(((est - e) / e) * 100).toFixed(2)}%`);
  }
  log("");
  log("Estimates are slightly HIGH (collisions from the long tail leak in) but the");
  log("ranking of heavy hitters is preserved — which is all 'top-K trending' needs.");
  log("Fixed memory, one-pass, mergeable across shards. That's why it's everywhere.");
  process.exit(0);
}

main();
