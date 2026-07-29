/**
 * Phase 4 — HYPERLOGLOG: counting UNIQUE items in tiny memory. Run: npm run phase4
 *
 * Count-Min (Phase 3) answers "how many times". HyperLogLog answers the other
 * classic: "how many DISTINCT?" — unique visitors, unique IPs. Exactly counting
 * uniques means remembering every one you've seen (a giant set). HLL estimates
 * cardinality in a couple KB, for ANY number of uniques, using one idea:
 *
 *   Hash each item to a random-looking bit string. In a stream of random bit
 *   strings, seeing one that STARTS WITH k zeros is roughly a 1-in-2^k event — so
 *   the max run of leading zeros you've observed hints at how many DISTINCT items
 *   passed through (duplicates hash the same, so they don't inflate it).
 *
 *   To cut variance, split into m "registers" by the first bits of the hash, keep
 *   the max leading-zeros per register, and combine with a harmonic mean. Error
 *   is ~1.04/√m — a few percent with a few thousand registers (a few KB).
 *
 * We feed a stream with duplicates and compare the HLL estimate to the true count.
 */

import { hash32, log } from "../lib/log.ts";

class HyperLogLog {
  private registers: Uint8Array;
  private p: number; // register-index bits → m = 2^p registers
  private m: number;
  constructor(p = 12) { this.p = p; this.m = 1 << p; this.registers = new Uint8Array(this.m); }

  add(item: string) {
    const h = hash32(item);
    const idx = h >>> (32 - this.p);          // first p bits pick the register
    const rest = (h << this.p) | (1 << (this.p - 1)); // remaining bits (guard bit so clz terminates)
    const leadingZeros = Math.clz32(rest) + 1; // position of leftmost 1
    if (leadingZeros > this.registers[idx]) this.registers[idx] = leadingZeros;
  }

  estimate(): number {
    const m = this.m;
    let sum = 0, zeros = 0;
    for (const r of this.registers) { sum += 2 ** -r; if (r === 0) zeros++; }
    const alpha = 0.7213 / (1 + 1.079 / m);
    let est = (alpha * m * m) / sum;
    if (est <= 2.5 * m && zeros > 0) est = m * Math.log(m / zeros); // small-range correction
    return Math.round(est);
  }
  bytes() { return this.m; }
}

function main() {
  const hll = new HyperLogLog(12); // 4096 registers ≈ 4KB, ~1.6% error
  const exact = new Set<string>();

  // 1,000,000 events but only ~200,000 distinct users (5x duplication).
  const DISTINCT = 200000;
  for (let i = 0; i < 1_000_000; i++) {
    const user = `user:${i % DISTINCT}`;
    hll.add(user);
    exact.add(user);
  }

  const est = hll.estimate();
  const truth = exact.size;
  log("═══ 1,000,000 events, but how many UNIQUE users? ═══");
  log("");
  log(`   EXACT set:    ${truth.toLocaleString()} uniques, storing ${truth.toLocaleString()} strings (~MBs of RAM)`);
  log(`   HYPERLOGLOG:  ${est.toLocaleString()} uniques, using ${hll.bytes().toLocaleString()} bytes (4KB, fixed)`);
  log(`   error: ${(Math.abs(est - truth) / truth * 100).toFixed(2)}%  (theoretical ~${(1.04 / Math.sqrt(hll.bytes()) * 100).toFixed(2)}% for this size)`);

  log("");
  log("A few KB to count uniques among a MILLION events, within a couple percent.");
  log("And HLLs MERGE (register-wise max), so each shard keeps its own and you union");
  log("them for a global unique count — no shuffling raw IDs around. That mergeability");
  log("is why 'daily unique visitors' at scale is almost always HLL, not a real set.");
  process.exit(0);
}

main();
