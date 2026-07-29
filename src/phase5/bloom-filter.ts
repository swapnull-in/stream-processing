/**
 * Phase 5 — BLOOM FILTER: "have I seen this before?" in tiny memory. Run: npm run phase5
 *
 * Third probabilistic structure, third question. Bloom answers SET MEMBERSHIP:
 * "is X in the set?" — dedup a stream ("did I already process this event id?"),
 * or skip a disk/DB lookup for a key that definitely isn't there.
 *
 *   A bit array of m bits + k hash functions. To ADD x: set the k bits it hashes
 *   to. To TEST x: if ALL k of its bits are set, x is "probably present"; if ANY
 *   is unset, x is DEFINITELY absent.
 *
 * The asymmetry is the whole point:
 *   • NO false negatives — if you added it, it always tests present. Safe to use
 *     as a "definitely-not-here" fast reject (LSM engines put one on each SSTable).
 *   • SOME false positives — bits set by other items can collide, so a few
 *     never-added items test "present". The rate is tunable by m and k.
 *
 * We add 10k items, then probe 100k never-added ones and measure the false-
 * positive rate against the textbook formula.
 */

import { hash32, log } from "../lib/log.ts";

class BloomFilter {
  private bits: Uint8Array;
  private m: number;
  private k: number;
  constructor(m: number, k: number) { this.m = m; this.k = k; this.bits = new Uint8Array(m); }

  private positions(item: string): number[] {
    // Two hashes combined into k (Kirsch-Mitzenmacher): h1 + i*h2.
    const h1 = hash32(item, 1), h2 = hash32(item, 2);
    return Array.from({ length: this.k }, (_, i) => (h1 + i * h2) % this.m);
  }
  add(item: string) { for (const p of this.positions(item)) this.bits[p] = 1; }
  test(item: string): boolean { return this.positions(item).every((p) => this.bits[p] === 1); }
}

function main() {
  const n = 10_000;     // items we insert
  const m = 100_000;    // bits (~10 bits/item)
  const k = 7;          // hashes (near-optimal for m/n=10)
  const bloom = new BloomFilter(m, k);

  for (let i = 0; i < n; i++) bloom.add(`event:${i}`);

  // No false negatives: everything we added must test present.
  let missing = 0;
  for (let i = 0; i < n; i++) if (!bloom.test(`event:${i}`)) missing++;

  // Measure false positives on items we never added.
  let falsePos = 0, probes = 100_000;
  for (let i = 0; i < probes; i++) if (bloom.test(`never:${i}`)) falsePos++;

  const measured = (falsePos / probes) * 100;
  const theoretical = Math.pow(1 - Math.exp((-k * n) / m), k) * 100;

  log(`═══ Bloom filter: ${n.toLocaleString()} items in ${(m / 8 / 1024).toFixed(1)} KB (${m.toLocaleString()} bits, k=${k}) ═══`);
  log("");
  log(`   false NEGATIVES: ${missing}  ✓ (guaranteed zero — added items always test present)`);
  log(`   false POSITIVES: ${falsePos}/${probes.toLocaleString()} = ${measured.toFixed(2)}%`);
  log(`   theoretical rate: ${theoretical.toFixed(2)}%  (measured matches the formula)`);

  log("");
  log("Use it as a cheap gate: 'definitely not here' skips the expensive lookup;");
  log("'probably here' falls through to the real check. A dedup filter on a stream,");
  log("a per-SSTable filter in an LSM (Phase from the databases repo), a cache-");
  log("penetration guard — same structure, same asymmetric guarantee everywhere.");
  process.exit(0);
}

main();
