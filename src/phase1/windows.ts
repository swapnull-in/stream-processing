/**
 * Phase 1 — WINDOWS: turning an endless stream into finite chunks. Run: npm run phase1
 *
 * A stream never ends, but "how many per minute?" needs a boundary. WINDOWS
 * slice the infinite stream into finite buckets you can aggregate. Three kinds:
 *
 *   TUMBLING — fixed, non-overlapping (every 60s). Each event lands in exactly
 *     one window. Use for "requests per minute", periodic rollups.
 *
 *   SLIDING — fixed size but overlapping, advancing by a smaller step (60s window
 *     every 10s). Each event is in MULTIPLE windows. Use for smooth moving
 *     averages / "in the last 60s, updated every 10s".
 *
 *   SESSION — dynamic, defined by GAPS of inactivity (new window after 30s idle).
 *     Use for "a user's browsing session". (Shown briefly at the end.)
 *
 * We run the same event stream through tumbling and sliding windows and count.
 */

import { log } from "../lib/log.ts";

interface Event { ts: number; user: string } // ts in seconds

// A stream of click events (timestamps in seconds).
const STREAM: Event[] = [
  { ts: 2, user: "a" }, { ts: 5, user: "b" }, { ts: 8, user: "a" },
  { ts: 12, user: "c" }, { ts: 18, user: "b" }, { ts: 21, user: "a" },
  { ts: 24, user: "d" }, { ts: 29, user: "b" }, { ts: 33, user: "a" },
];

function tumbling(events: Event[], sizeSec: number) {
  const windows = new Map<number, number>();
  for (const e of events) {
    const w = Math.floor(e.ts / sizeSec) * sizeSec; // window start
    windows.set(w, (windows.get(w) ?? 0) + 1);
  }
  return windows;
}

function sliding(events: Event[], sizeSec: number, slideSec: number) {
  const maxTs = Math.max(...events.map((e) => e.ts));
  const windows = new Map<number, number>();
  for (let start = 0; start <= maxTs; start += slideSec) {
    const count = events.filter((e) => e.ts >= start && e.ts < start + sizeSec).length;
    windows.set(start, count);
  }
  return windows;
}

function main() {
  log("═══ TUMBLING windows (size 10s, non-overlapping) ═══");
  for (const [start, count] of tumbling(STREAM, 10))
    log(`   [${start}s–${start + 10}s): ${count} events`);
  log("   each event counted in exactly ONE window; windows tile the timeline.");

  log("");
  log("═══ SLIDING windows (size 10s, slide 5s — overlapping) ═══");
  for (const [start, count] of sliding(STREAM, 10, 5))
    log(`   [${start}s–${start + 10}s): ${count} events`);
  log("   windows overlap by 5s, so most events are counted in TWO windows —");
  log("   that overlap is what makes a smooth moving metric.");

  log("");
  log("═══ SESSION windows (gap > 4s starts a new session) ═══");
  const gap = 4;
  let sessions = 1, prev = STREAM[0].ts;
  for (const e of STREAM.slice(1)) { if (e.ts - prev > gap) sessions++; prev = e.ts; }
  log(`   ${sessions} sessions detected (boundaries wherever the stream went quiet > ${gap}s)`);

  log("");
  log("Windows are the first decision in any stream job. The next question — which");
  log("CLOCK defines a window, the event's own time or the wall clock when it arrives —");
  log("is where the real subtlety lives (Phase 2).");
  process.exit(0);
}

main();
