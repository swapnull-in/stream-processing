/**
 * Phase 6 — THE ROLLUP CUBE: "rollup, never recount". Run: npm run phase6
 *
 * The last idea ties the analytics story together. You never answer dashboard
 * queries by scanning raw events — there are too many. Instead you PRE-AGGREGATE
 * in a hierarchy and always build coarser grains from finer ones:
 *
 *   raw events  →  per-MINUTE rollups  →  per-HOUR rollups  →  per-DAY …
 *
 * Two rules make it work:
 *   • ROLLUP, NEVER RECOUNT: the hourly number is the SUM of its 60 minute
 *     buckets — you never touch raw events again to compute it. Each grain is
 *     built once from the grain below. Counts are additive, so this is exact.
 *   • DIMENSIONS form a CUBE: roll up by (time × country × device …). A query
 *     slices/aggregates the cube instead of the firehose.
 *
 * We ingest raw events once into minute buckets, then derive hours from minutes
 * (not from raw), and show queries answered by reading the cube.
 */

import { log } from "../lib/log.ts";

interface Event { ts: number; country: string } // ts = seconds

// Raw stream: page views tagged by country, over ~2 hours.
function generateEvents(): Event[] {
  const countries = ["US", "IN", "UK", "US", "US", "IN"]; // US-heavy
  const events: Event[] = [];
  let seed = 12345;
  const rand = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
  for (let i = 0; i < 20000; i++) events.push({ ts: Math.floor(rand() * 7200), country: countries[Math.floor(rand() * countries.length)] });
  return events;
}

function main() {
  const events = generateEvents();

  // ─── Stage 1: raw → per-minute cube keyed by (minute, country) ─────────────
  const minuteCube = new Map<string, number>(); // "minute|country" → count
  for (const e of events) {
    const minute = Math.floor(e.ts / 60);
    minuteCube.set(`${minute}|${e.country}`, (minuteCube.get(`${minute}|${e.country}`) ?? 0) + 1);
  }
  log(`═══ Stage 1: rolled ${events.length.toLocaleString()} raw events → ${minuteCube.size} minute×country cells ═══`);

  // ─── Stage 2: per-minute → per-hour, by SUMMING minutes (never recount raw) ─
  const hourCube = new Map<string, number>();
  for (const [key, count] of minuteCube) {
    const [minute, country] = key.split("|");
    const hour = Math.floor(Number(minute) / 60);
    hourCube.set(`${hour}|${country}`, (hourCube.get(`${hour}|${country}`) ?? 0) + count); // sum, don't rescan
  }
  log(`   Stage 2: rolled minutes → ${hourCube.size} hour×country cells (by SUMMING minutes, not re-reading raw)`);

  // ─── Queries answered from the cube ────────────────────────────────────────
  log("");
  log("═══ Dashboard queries — all served from the cube, no raw scan ═══");
  const hour0US = hourCube.get("0|US") ?? 0;
  log(`   views from US in hour 0:        ${hour0US.toLocaleString()}  (one cube lookup)`);

  let hour0All = 0;
  for (const [key, c] of hourCube) if (key.startsWith("0|")) hour0All += c;
  log(`   total views in hour 0 (all countries): ${hour0All.toLocaleString()}  (slice the country dimension)`);

  // Verify rollup correctness: hour 0 US == sum of its minute buckets == raw count.
  let fromMinutes = 0;
  for (const [key, c] of minuteCube) { const [m, ctry] = key.split("|"); if (ctry === "US" && Number(m) < 60) fromMinutes += c; }
  const fromRaw = events.filter((e) => e.country === "US" && e.ts < 3600).length;
  log("");
  log(`   consistency check — US hour 0: cube=${hour0US}, sum-of-minutes=${fromMinutes}, raw-recount=${fromRaw}`);
  log(`   ${hour0US === fromMinutes && fromMinutes === fromRaw ? "✓ all equal — rollups are exact (counts are additive)" : "✗ mismatch"}`);

  log("");
  log("The raw firehose was touched exactly ONCE. Every grain above is built from");
  log("the grain below, and every dashboard query reads a few cube cells instead of");
  log("millions of events. That's real-time analytics: pre-aggregate, then slice.");
  process.exit(0);
}

main();
