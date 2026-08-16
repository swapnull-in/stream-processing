/**
 * Phase 9 — JOINS IN STREAMS (§2.10). Run: node src/phase9/stream-joins.ts
 *
 * Joining is trivial in a database: both tables sit still, you match rows. In a
 * stream both sides are UNBOUNDED and never stop arriving, so you can't hold all
 * of both forever. Three shapes, each with a different cost:
 *
 *   STREAM–STREAM  — both sides are streams. To match them you must BUFFER both
 *     sides' recent state for a time WINDOW and join within it (event-time).
 *   STREAM–TABLE   — a stream enriched from a dimension table (a user's tier, a
 *     product's price). The table is a lookup — but its values CHANGE over time.
 *   TABLE–TABLE    — both sides are changelog "tables"; the join result is
 *     maintained continuously as either side updates (materialized view).
 *
 * We demonstrate the two that bite people, and their traps:
 *
 *   A) The stream–stream WINDOWED join, and the click that arrives too late to
 *      attribute (state = window × throughput — a memory bomb if the window is wide).
 *   B) The stream–table TEMPORAL (as-of) join, and why a naive "look up the CURRENT
 *      value" enrichment is non-reproducible on replay — the SCD problem, returned.
 */

import { log } from "../lib/log.ts";

// ─────────────────────────────────────────────────────────────────────────────
// A) STREAM–STREAM WINDOWED JOIN — ad attribution (impression ⋈ click)
// ─────────────────────────────────────────────────────────────────────────────
//
// We attribute a click to an earlier impression of the SAME ad when the click's
// event time falls within a window after the impression. Both streams are
// unbounded, so we keep a bounded BUFFER of recent impressions keyed by adId and
// evict anything older than the window. That buffer IS the join's state — and it
// grows with (window × arrival rate). Widen the window on a busy stream and you
// blow up memory; bound it tightly.

interface Impression { eventTime: number; adId: string; id: string } // seconds
interface Click { eventTime: number; adId: string; id: string }

const JOIN_WINDOW = 30; // attribute a click to an impression at most 30s earlier

// A single event-time-ordered arrival log interleaving both streams.
const IMPRESSIONS: Impression[] = [
  { eventTime: 10, adId: "A", id: "imp-A" },
  { eventTime: 12, adId: "B", id: "imp-B" },
];
const CLICKS: Click[] = [
  { eventTime: 25, adId: "A", id: "clk-A" }, // 25 − 10 = 15s ≤ 30 → MATCH
  { eventTime: 50, adId: "B", id: "clk-B" }, // 50 − 12 = 38s > 30 → too late, MISS
];

function streamStreamJoin() {
  log("═══ A) STREAM–STREAM windowed join: impressions ⋈ clicks (window 30s) ═══");

  // Buffered join state: adId → the recent impression. Bounded by the window.
  const buffer = new Map<string, Impression>();

  // Merge both sides and process in event-time order (as a real join would see them).
  const arrivals = [...IMPRESSIONS.map((e) => ({ kind: "imp" as const, e })),
                    ...CLICKS.map((e) => ({ kind: "clk" as const, e }))]
    .sort((a, b) => a.e.eventTime - b.e.eventTime);

  for (const { kind, e } of arrivals) {
    // Evict impressions that can no longer be joined — nothing arriving at or
    // after `e.eventTime` could be within the window of them. This eviction is
    // what KEEPS THE STATE BOUNDED (to window × throughput) instead of forever.
    for (const [adId, imp] of buffer) {
      if (e.eventTime - imp.eventTime > JOIN_WINDOW) {
        buffer.delete(adId);
        log(`      ⌫ evict ${imp.id} (event-time ${imp.eventTime}s) — past the ${JOIN_WINDOW}s window, can't match anymore`);
      }
    }

    if (kind === "imp") {
      buffer.set(e.adId, e);
      log(`   ${e.id} @ ${e.eventTime}s → buffered, waiting up to ${JOIN_WINDOW}s for a click on ad ${e.adId}`);
    } else {
      const imp = buffer.get(e.adId);
      const gap = imp ? e.eventTime - imp.eventTime : Infinity;
      if (imp && gap <= JOIN_WINDOW) {
        log(`   ${e.id} @ ${e.eventTime}s → ✓ MATCH ${imp.id} (gap ${gap}s ≤ ${JOIN_WINDOW}s) → attribute click to impression`);
      } else {
        log(`   ${e.id} @ ${e.eventTime}s → ✗ NO MATCH (gap ${gap === Infinity ? "n/a" : gap + "s"} > ${JOIN_WINDOW}s) → click OUTSIDE window, NOT attributed`);
      }
    }
  }

  log("   clk-A matched; clk-B fired 38s after its impression — beyond the 30s window,");
  log("   so the impression had already been evicted and the click misses the join.");
  log("   COST: the buffer holds every impression for the whole window, so its size is");
  log("   window × throughput. A wide window on a high-volume stream is a memory bomb —");
  log("   bound the window tightly, or spill state to disk/RocksDB.");
}

// ─────────────────────────────────────────────────────────────────────────────
// B) STREAM–TABLE ENRICHMENT + THE TEMPORAL / AS-OF TRAP  (the staff point)
// ─────────────────────────────────────────────────────────────────────────────
//
// We enrich order events with a product's price from a dimension "table". But the
// price CHANGES over time, so the table is really a VERSIONED history. A naive
// enrichment looks up the CURRENT price — which gives a different answer every time
// the table changes, so a REPLAY of the same order produces a different total.
// Non-reproducible. The fix is a TEMPORAL / as-of join: look up the price that was
// valid AS OF THE ORDER'S EVENT TIME. That's a fixed, reproducible answer forever.
// It is the slowly-changing-dimension (SCD) problem, reappearing in streaming.

interface PriceVersion { validFrom: number; price: number } // seconds
interface Order { eventTime: number; product: string; qty: number; id: string }

// Price history for product P1 (append-only, ordered by validFrom).
const PRICE_HISTORY: PriceVersion[] = [
  { validFrom: 0, price: 100 },
  { validFrom: 20, price: 120 },
  { validFrom: 40, price: 150 }, // ← the CURRENT price (latest version)
];

const ORDERS: Order[] = [
  { eventTime: 5, product: "P1", qty: 2, id: "ord-1" },  // as-of price 100
  { eventTime: 25, product: "P1", qty: 1, id: "ord-2" }, // as-of price 120
];

/** Latest price overall — what a naive "current value" lookup returns. */
function currentPrice(): number {
  return PRICE_HISTORY[PRICE_HISTORY.length - 1].price;
}

/** As-of join: the price whose validFrom is the greatest one ≤ the event time. */
function priceAsOf(t: number): number {
  let chosen = PRICE_HISTORY[0].price;
  for (const v of PRICE_HISTORY) if (v.validFrom <= t) chosen = v.price;
  return chosen;
}

function streamTableJoin() {
  log("");
  log("═══ B) STREAM–TABLE enrichment: order ⋈ price — NAIVE vs TEMPORAL (as-of) ═══");
  log(`   price history: ${PRICE_HISTORY.map((v) => `@${v.validFrom}s=$${v.price}`).join("  ")}   (current = $${currentPrice()})`);

  for (const o of ORDERS) {
    const cur = currentPrice();
    const asof = priceAsOf(o.eventTime);
    const naiveTotal = cur * o.qty;
    const asofTotal = asof * o.qty;
    log(`   ${o.id} @ ${o.eventTime}s ×${o.qty}:`);
    log(`      naive (CURRENT price $${cur}) → $${naiveTotal}   ← changes whenever the table changes → NOT reproducible on replay`);
    log(`      as-of (price valid @ ${o.eventTime}s = $${asof}) → $${asofTotal}   ← the price that was true when the order happened → reproducible ✓`);
  }

  log("   ord-2 happened at 25s, when the price was $120 — but the CURRENT price is now");
  log("   $150. The naive join bills it at $150 and would bill it differently again after");
  log("   the next price change. The as-of join always bills $120: a replay is identical.");
}

// ─────────────────────────────────────────────────────────────────────────────
// C) The three join shapes side by side.
// ─────────────────────────────────────────────────────────────────────────────

function comparisonTable() {
  log("");
  log("═══ C) The three stream join shapes ═══");
  log("   ┌───────────────┬──────────────────────────┬─────────────┬──────────────────────────┐");
  log("   │ join          │ what's held               │ time model  │ result                   │");
  log("   ├───────────────┼──────────────────────────┼─────────────┼──────────────────────────┤");
  log("   │ stream–stream │ BOTH sides buffered for a │ event-time  │ matched pairs within the │");
  log("   │               │ window (state=win×tput)   │ window      │ window; late events miss │");
  log("   │ stream–table  │ the table (latest OR      │ temporal /  │ each event enriched with │");
  log("   │               │ versioned history)        │ as-of       │ the as-of dimension value│");
  log("   │ table–table   │ BOTH sides as changelogs  │ continuous  │ a materialized view, kept│");
  log("   │               │ (full state each side)    │ (no window) │ up to date as either side│");
  log("   └───────────────┴──────────────────────────┴─────────────┴──────────────────────────┘");
  log("   Note: for windowed joins, a LATE event on EITHER side (its match already evicted)");
  log("   misses the join window entirely — the same lateness problem as windowed aggregation.");
}

function main() {
  streamStreamJoin();
  streamTableJoin();
  comparisonTable();

  log("");
  log("TAKEAWAY: stream–stream joins buffer BOTH sides for a window, so state = window ×");
  log("throughput — bound the window tightly or it's a memory bomb. stream–table enrichment");
  log("must be a TEMPORAL / as-of join: join against the dimension value AS OF the event's");
  log("event time, not the current value, or replays produce different results (the SCD");
  log("problem). Late events on either side miss the join window.");
  process.exit(0);
}

main();
