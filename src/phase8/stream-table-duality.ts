/**
 * Phase 8 — STREAM–TABLE DUALITY & CDC. Run: npm run phase8
 *
 * The conceptual key to every modern data system. Two shapes for the same data:
 *
 *   A STREAM is a sequence of CHANGES — an append-only changelog. Facts, in order.
 *   A TABLE  is a SNAPSHOT of current state — the result of applying all changes.
 *
 * They are two views of one thing, and you can turn either into the other:
 *
 *   STREAM → TABLE : FOLD the changelog, keeping the latest value per key. That
 *     "GROUP BY key, take last" is an upsert. In Kafka terms: a KStream (every
 *     record) collapsed into a KTable (latest value per key).
 *   TABLE → STREAM : emit every change to the table as records — the table's
 *     CHANGELOG. Re-folding that changelog reproduces the table exactly (a
 *     round-trip). This is Change Data Capture (CDC).
 *
 * Three consequences we demonstrate:
 *   • LOG COMPACTION — a compacted topic garbage-collects old values, keeping
 *     only the latest record per key. N raw records shrink to K distinct keys.
 *     The compacted log literally IS the table.
 *   • CDC — a database's WAL / binlog is already a stream. A tiny "database" (a
 *     Map) emits change events (op, key, before, after) on every insert/update/
 *     delete. A consumer folds them back into a byte-identical replica.
 *   • A DELETE becomes a TOMBSTONE — a record with a null value that erases the
 *     key on both the compacted log and the replica.
 *
 * This is why Kappa keeps the log as the source of truth and materializes tables
 * on demand, and why Debezium can turn an OLTP table into an ordered event stream
 * feeding the warehouse, search index, and caches — retiring fragile dual writes.
 */

import { log } from "../lib/log.ts";

// ─── A) STREAM → TABLE ───────────────────────────────────────────────────────
// An append-only changelog of account-balance changes. Multiple records per key;
// order matters — later records overwrite earlier ones for the same key.
interface Record_ { key: string; value: number | null } // value=null is a tombstone (a delete)

const CHANGELOG: Record_[] = [
  { key: "alice", value: 100 },
  { key: "bob", value: 50 },
  { key: "alice", value: 140 }, // alice tops up
  { key: "carol", value: 30 },
  { key: "bob", value: 20 },    // bob spends
  { key: "alice", value: 175 }, // alice again — only this survives for 'alice'
  { key: "carol", value: 30 },  // no-op re-write, still a record on the stream
];

/** FOLD a changelog into a table: latest value per key (a KTable). Tombstones delete. */
function fold(records: Record_[]): Map<string, number> {
  const table = new Map<string, number>();
  for (const r of records) {
    if (r.value === null) table.delete(r.key); // tombstone erases the key
    else table.set(r.key, r.value);            // upsert: latest wins
  }
  return table;
}

/** Serialize a table deterministically so two tables can be compared for equality. */
function serialize(table: Map<string, number>): string {
  return [...table.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => `${k}=${v}`).join(", ");
}

function main() {
  // ─── A) STREAM → TABLE (fold / upsert by key) ──────────────────────────────
  log("═══ A) STREAM → TABLE — fold an append-only changelog into latest-per-key ═══");
  log(`   KStream (every record, in order) — ${CHANGELOG.length} records:`);
  for (const r of CHANGELOG) log(`      + ${r.key} → ${r.value}`);
  const table = fold(CHANGELOG);
  log(`   KTable (latest value per key) — ${table.size} rows:`);
  for (const [k, v] of table) log(`      ${k} = ${v}`);
  log(`   → ${CHANGELOG.length} stream records GROUP BY key = ${table.size}-row table {${serialize(table)}}`);

  // ─── B) TABLE → STREAM (changelog / CDC round-trip) ────────────────────────
  log("");
  log("═══ B) TABLE → STREAM — materialize the table's changelog, then re-fold ═══");
  // The changelog we already have IS the table's stream of changes. Ship it to a
  // consumer, who folds it back. Same input → same table. That's the round-trip.
  const rebuilt = fold(CHANGELOG);
  const same = serialize(table) === serialize(rebuilt);
  log(`   original table : {${serialize(table)}}`);
  log(`   re-folded copy : {${serialize(rebuilt)}}`);
  log(`   ${same ? "✓ round-trip is exact — a stream IS the table's changelog, a table IS a materialized stream" : "✗ mismatch"}`);

  // ─── C) LOG COMPACTION (raw log N → compacted log K) ───────────────────────
  log("");
  log("═══ C) LOG COMPACTION — keep only the latest record per key ═══");
  // Compaction walks the log and, per key, retains just the last offset. The
  // compacted log is exactly the set of records the fold in (A) kept.
  const latestByKey = new Map<string, Record_>();
  for (const r of CHANGELOG) latestByKey.set(r.key, r); // last write per key wins
  const compacted = [...latestByKey.values()];
  log(`   raw log:       ${CHANGELOG.length} records  [${CHANGELOG.map((r) => r.key).join(", ")}]`);
  log(`   compacted log: ${compacted.length} records  [${compacted.map((r) => `${r.key}=${r.value}`).join(", ")}]`);
  log(`   → compaction ${CHANGELOG.length} → ${compacted.length}: old values for a key are garbage-collected.`);
  const compactedIsTable = serialize(fold(compacted)) === serialize(table);
  log(`   ${compactedIsTable ? "✓ folding the compacted log gives the SAME table — the compacted topic BE a table" : "✗ mismatch"}`);

  // ─── D) CDC — a database's WAL is a stream (Debezium reading the binlog) ────
  log("");
  log("═══ D) CDC — a database's WAL/binlog IS a stream; fold it into a replica ═══");
  // A tiny "database": a Map. Every write appends a CDC change event to the WAL.
  // op = c(reate) / u(pdate) / d(elete); before/after are the row images.
  interface Change { op: "c" | "u" | "d"; key: string; before: number | null; after: number | null }
  const db = new Map<string, number>();
  const wal: Change[] = [];

  function apply(op: "c" | "u" | "d", key: string, value: number | null) {
    const before = db.has(key) ? db.get(key)! : null;
    if (op === "d") db.delete(key);
    else db.set(key, value!);
    const after = op === "d" ? null : value;
    wal.push({ op, key, before, after });
    const label = op === "c" ? "INSERT" : op === "u" ? "UPDATE" : "DELETE";
    log(`      ${label.padEnd(6)} ${key.padEnd(6)} before=${String(before).padStart(4)}  after=${String(after).padStart(4)}`);
  }

  log("   OLTP writes (each emits a CDC event onto the WAL):");
  apply("c", "x9001", 100); // new order row
  apply("c", "x9002", 250);
  apply("u", "x9001", 175); // customer edits order
  apply("c", "x9003", 60);
  apply("d", "x9002", null); // order cancelled → tombstone
  apply("u", "x9003", 90);

  // A downstream consumer (warehouse / search index / cache) folds the WAL into a
  // replica table — never touching the primary DB. A 'd' event is a tombstone.
  log("   Consumer folds the WAL stream into a replica table:");
  const replica = new Map<string, number>();
  for (const c of wal) {
    if (c.op === "d") replica.delete(c.key);
    else replica.set(c.key, c.after!);
  }
  log(`      source DB : {${serialize(db)}}`);
  log(`      replica   : {${serialize(replica)}}`);
  const replicated = serialize(db) === serialize(replica);
  log(`   ${replicated ? "✓ replica matches source — the WAL stream reconstructed the table with no dual write" : "✗ mismatch"}`);
  // FOOTGUN: a CDC connector holds a replication slot. If the consumer lags (or
  // dies), Postgres cannot recycle WAL past the slot's position — the WAL grows
  // unbounded until the disk fills and the primary halts. Monitor slot lag.

  log("");
  log("TAKEAWAY: every table is a stream and every stream is a table. A table is a");
  log("materialized stream (fold the changelog, latest-per-key); a stream is a");
  log("table's changelog (CDC). Log compaction makes a topic BE a table. A database's");
  log("WAL is a stream — which is why Debezium turns an OLTP table into an ordered");
  log("event stream feeding the warehouse, search, and caches, retiring dual writes,");
  log("and why Kappa keeps the log as source of truth and materializes tables on demand.");
  process.exit(0);
}

main();
