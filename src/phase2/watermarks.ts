/**
 * Phase 2 — EVENT TIME, PROCESSING TIME & WATERMARKS. Run: npm run phase2
 *
 * The single most important idea in stream processing. Two clocks:
 *
 *   EVENT TIME — when the event actually happened (stamped at the source).
 *   PROCESSING TIME — when your system got around to seeing it.
 *
 * They differ, because networks delay and reorder. A click at 12:00:59 might
 * arrive at 12:01:03 — after the "12:00 minute" window you'd naively close. If
 * you bucket by PROCESSING time, that click is counted in the wrong minute. Bug.
 * So you window by EVENT time. But then: when is the 12:00 window "done", given a
 * straggler could still arrive? You can't wait forever.
 *
 *   WATERMARK — a moving assertion: "I believe I've now seen all events up to
 *   time T." It's usually maxEventTimeSeen − allowedLateness. When the watermark
 *   passes a window's end, you CLOSE and emit that window. An event whose event
 *   time is behind the watermark is LATE — drop it, or send it to a side output.
 *
 * We feed an out-of-order stream and watch windows close on the watermark, with
 * one late event arriving after its window already fired.
 */

import { log } from "../lib/log.ts";

interface Event { eventTime: number; id: string } // seconds

// Arrival ORDER (processing order) is scrambled vs event time — note #e6 at t=8.
const ARRIVALS: Event[] = [
  { eventTime: 2, id: "e1" }, { eventTime: 5, id: "e2" }, { eventTime: 9, id: "e3" },
  { eventTime: 7, id: "e4" },  // slightly out of order (arrived after e3)
  { eventTime: 12, id: "e5" }, { eventTime: 8, id: "e6" }, // e6 is LATE (window [0,10) may be closed)
  { eventTime: 15, id: "e7" }, { eventTime: 19, id: "e8" },
];

const WINDOW = 10;          // tumbling windows of 10s in EVENT time
const ALLOWED_LATENESS = 2; // watermark lags maxEventTime by 2s

function main() {
  const windowCounts = new Map<number, string[]>(); // window start → event ids
  const closed = new Set<number>();
  let watermark = -Infinity;
  let maxEventTime = -Infinity;

  log("═══ Processing an out-of-order stream by EVENT time ═══");
  for (const e of ARRIVALS) {
    const wStart = Math.floor(e.eventTime / WINDOW) * WINDOW;

    if (closed.has(wStart)) {
      log(`   ⚠ ${e.id} (event-time ${e.eventTime}s) is LATE — window [${wStart},${wStart + WINDOW}) already closed → dropped`);
      continue;
    }
    (windowCounts.get(wStart) ?? windowCounts.set(wStart, []).get(wStart)!).push(e.id);
    log(`   ${e.id} @ event-time ${e.eventTime}s → window [${wStart},${wStart + WINDOW})`);

    // Advance the watermark and close any window whose end is now behind it.
    maxEventTime = Math.max(maxEventTime, e.eventTime);
    watermark = maxEventTime - ALLOWED_LATENESS;
    for (const [start, ids] of windowCounts) {
      if (!closed.has(start) && watermark >= start + WINDOW) {
        closed.add(start);
        log(`      ⟶ watermark ${watermark}s passed ${start + WINDOW}s → CLOSE [${start},${start + WINDOW}): ${ids.length} events {${ids.join(",")}}`);
      }
    }
  }

  log("");
  log("Key moments: e4 (t=7) arrived AFTER e3 (t=9) but still landed in the right");
  log("window — because we bucket by event time, not arrival order. And e6 (t=8)");
  log("arrived after the [0,10) window had closed on the watermark, so it was LATE.");
  log("");
  log("The three ways to handle late events: (1) drop them (what we did), (2) send");
  log("to a side output for a correction job, (3) keep the window open longer");
  log("(bigger allowed-lateness) — trading freshness for completeness.");
  process.exit(0);
}

main();
