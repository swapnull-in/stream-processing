/**
 * Phase 12 — SCHEMA EVOLUTION & THE REGISTRY (§6.5). Run: node "src/phase12/schema-evolution.ts"
 *
 * The governance crux of any multi-team pipeline. One team owns the producer for
 * `ClickEvent`; fifty other teams consume it. If that one team RENAMES a field,
 * every consumer that reads the old name breaks at once — a coordinated,
 * cross-team deploy, the thing that makes big pipelines impossible to change.
 *
 * A SCHEMA REGISTRY fixes this. It's a small service that stores VERSIONED
 * schemas per subject and ENFORCES a compatibility rule at REGISTER time — before
 * a single bad byte is produced. Safe changes are accepted; breaking ones are
 * rejected at the door, so the pipeline never has to be un-broken after the fact.
 *
 *   COMPATIBILITY MODES — which direction of reader/writer skew must keep working:
 *     • BACKWARD — a NEW-schema consumer can read OLD data. Allows: add an
 *       optional field (with default), or drop a field. → upgrade CONSUMERS first.
 *     • FORWARD  — an OLD-schema consumer can read NEW data. Allows: add a field,
 *       or drop an optional one. → upgrade PRODUCERS first.
 *     • FULL     — both must hold. → deploy either side first, any order.
 *   BACKWARD is the usual default: it lets you evolve producers freely once
 *   consumers are ready, and consumers can lag without breaking.
 *
 *   SCHEMA ID, NOT FULL SCHEMA — a produced record embeds a tiny numeric schema
 *   ID, not its whole schema (that would bloat every message). The consumer reads
 *   the ID, FETCHES that exact writer schema from the registry, and deserializes
 *   against it — filling defaults for fields its own reader schema adds, ignoring
 *   fields its reader schema doesn't know. Avro and Protobuf are the real formats
 *   this models; CDC sources evolve too — an `ALTER TABLE` upstream must propagate
 *   through the registry just like an application-level field change.
 *
 * We stand up a tiny in-process registry and show: (A) a backward-compatible
 * optional-add cross-read by old and new consumers with no coordinated deploy,
 * (B) a rename REJECTED at registration and the migration that replaces it, and
 * (C) one change judged by all three modes to make the upgrade-order concrete.
 */

import { log } from "../lib/log.ts";

// A field is name + type + whether it's optional, plus a default used to fill it
// in when a reader expects the field but the writer's data didn't carry it.
interface Field { name: string; type: string; optional: boolean; default?: unknown }
interface Schema { subject: string; version: number; id: number; fields: Field[] }
type Mode = "BACKWARD" | "FORWARD" | "FULL";
type CompatResult = { ok: true; id: number; version: number } | { ok: false; reasons: string[] };

// A produced message carries a small schema ID, NOT the schema itself.
interface Message { schemaId: number; data: Record<string, unknown> }

const byName = (fields: Field[]) => new Map(fields.map((f) => [f.name, f]));

/**
 * The compatibility engine. Given the PREVIOUS registered schema and a proposed
 * NEXT one, list every reason the change would break reading in each direction.
 *   backward reasons — a NEW-schema consumer failing to read OLD data.
 *   forward reasons  — an OLD-schema consumer failing to read NEW data.
 * A rename shows up as {drop old field} + {add new field without default}: the
 * add is what breaks backward, the drop is what breaks forward — breaking both.
 */
function compatReasons(prev: Field[], next: Field[]): { backward: string[]; forward: string[] } {
  const p = byName(prev), n = byName(next);
  const backward: string[] = [], forward: string[] = [];

  for (const f of next) {
    if (!p.has(f.name)) {
      // Added field. New consumer reading OLD data won't find it → needs a default.
      if (f.default === undefined) backward.push(`adds required field '${f.name}' with no default — a new-schema consumer can't fill it when reading old data`);
    } else if (p.get(f.name)!.type !== f.type) {
      // Type change (e.g. narrowing string→int) is unsafe in BOTH directions.
      const t = p.get(f.name)!.type;
      backward.push(`changes type of '${f.name}' (${t}→${f.type}) — old values may not fit the new type`);
      forward.push(`changes type of '${f.name}' (${t}→${f.type}) — new values may not fit the old type`);
    }
  }
  for (const f of prev) {
    if (!n.has(f.name)) {
      // Removed field. Old consumer reading NEW data won't find it → old schema must have had a default.
      if (f.default === undefined) forward.push(`removes field '${f.name}' which had no default — an old-schema consumer can't fill it when reading new data`);
    }
  }
  return { backward, forward };
}

class SchemaRegistry {
  private byId = new Map<number, Schema>();
  private bySubject = new Map<string, Schema[]>();
  private nextId = 1;

  latest(subject: string): Schema | undefined {
    const versions = this.bySubject.get(subject);
    return versions?.[versions.length - 1];
  }
  getById(id: number): Schema {
    const s = this.byId.get(id);
    if (!s) throw new Error(`no schema with id ${id}`);
    return s;
  }

  /** Register a new schema version under `subject`, enforcing `mode` vs the previous version. */
  register(subject: string, fields: Field[], mode: Mode): CompatResult {
    const prev = this.latest(subject);
    if (prev) {
      const { backward, forward } = compatReasons(prev.fields, fields);
      // Which reasons matter depends on the mode we're enforcing.
      const violations =
        mode === "BACKWARD" ? backward :
        mode === "FORWARD" ? forward :
        [...backward, ...forward]; // FULL — both directions must hold
      if (violations.length > 0) return { ok: false, reasons: violations };
    }
    const id = this.nextId++;
    const version = (prev?.version ?? 0) + 1;
    const schema: Schema = { subject, version, id, fields };
    this.byId.set(id, schema);
    (this.bySubject.get(subject) ?? this.bySubject.set(subject, []).get(subject)!).push(schema);
    return { ok: true, id, version };
  }
}

/** Producer side: embed only the schema ID with the record's data. */
function produce(schemaId: number, data: Record<string, unknown>): Message {
  return { schemaId, data };
}

/**
 * Consumer side: fetch the WRITER schema by the ID on the message, then read it
 * through the consumer's own READER schema — filling defaults for fields the
 * reader adds, ignoring fields the reader doesn't know about.
 */
function consume(reg: SchemaRegistry, msg: Message, reader: Schema): Record<string, unknown> {
  reg.getById(msg.schemaId); // in a real client the writer schema decodes the bytes; here data is already keyed
  const out: Record<string, unknown> = {};
  for (const f of reader.fields) {
    out[f.name] = f.name in msg.data ? msg.data[f.name] : f.default; // default fills a missing field
  }
  return out;
}

function main() {
  const reg = new SchemaRegistry();

  // ─── v1: the original ClickEvent ────────────────────────────────────────────
  const v1Fields: Field[] = [
    { name: "userId", type: "string", optional: false },
    { name: "pageId", type: "string", optional: false },
    { name: "ts", type: "number", optional: false },
  ];
  const v1 = reg.register("ClickEvent", v1Fields, "BACKWARD");
  log("═══ A) BACKWARD-COMPATIBLE optional-add — no coordinated deploy ═══");
  log(`   registered ClickEvent v1 (id ${(v1 as { id: number }).id}): {userId, pageId, ts}`);

  // ─── v2: add an OPTIONAL field with a DEFAULT → backward-compatible ─────────
  const v2Fields: Field[] = [
    ...v1Fields,
    { name: "campaignId", type: "string", optional: true, default: null },
  ];
  const v2 = reg.register("ClickEvent", v2Fields, "BACKWARD");
  log(`   register v2 = v1 + optional campaignId (default null): ${v2.ok ? `✓ ACCEPTED as id ${v2.id}` : "✗ rejected"}`);

  const v1Schema = reg.getById((v1 as { id: number }).id);
  const v2Schema = reg.getById((v2 as { id: number }).id);

  // Two producers on two schema versions, each embedding just its schema ID.
  const oldMsg = produce(v1Schema.id, { userId: "u1", pageId: "/home", ts: 1000 });
  const newMsg = produce(v2Schema.id, { userId: "u2", pageId: "/pricing", ts: 2000, campaignId: "summer" });

  // Cross-read: OLD consumer reads NEW data (ignores campaignId it doesn't know)…
  const oldReadsNew = consume(reg, newMsg, v1Schema);
  log(`   OLD consumer (v1) reads NEW data (msg schemaId ${newMsg.schemaId}) → ${JSON.stringify(oldReadsNew)}  (campaignId ignored)`);
  // …and NEW consumer reads OLD data (default fills the missing campaignId).
  const newReadsOld = consume(reg, oldMsg, v2Schema);
  log(`   NEW consumer (v2) reads OLD data (msg schemaId ${oldMsg.schemaId}) → ${JSON.stringify(newReadsOld)}  (campaignId defaulted)`);
  log("   → both directions work, so you can upgrade consumers first and producers later — nobody coordinates.");

  // ─── B) A breaking RENAME is rejected at registration ───────────────────────
  log("");
  log("═══ B) BREAKING CHANGE (rename) — rejected before any bad data is produced ═══");
  const renameFields: Field[] = [
    { name: "userId", type: "string", optional: false },
    { name: "pageUrl", type: "string", optional: false }, // pageId RENAMED to pageUrl
    { name: "ts", type: "number", optional: false },
    { name: "campaignId", type: "string", optional: true, default: null },
  ];
  const rename = reg.register("ClickEvent", renameFields, "BACKWARD");
  if (!rename.ok) {
    log("   register v3 renaming pageId→pageUrl under BACKWARD: ✗ REJECTED");
    for (const r of rename.reasons) log(`      • ${r}`);
  }
  log("   a rename = drop 'pageId' + add 'pageUrl' with no default; the registry blocks it at the door.");

  // The fix: not a rename — a MIGRATION. Add pageUrl as a NEW optional field.
  const migrateFields: Field[] = [
    ...v2Fields,
    { name: "pageUrl", type: "string", optional: true, default: "" }, // additive, backfill-able
  ];
  const v3 = reg.register("ClickEvent", migrateFields, "BACKWARD");
  log(`   fix — add pageUrl as a NEW optional field (default ""): ${v3.ok ? `✓ ACCEPTED as v${v3.version}` : "✗ rejected"}`);
  log("   then backfill pageUrl from pageId, move consumers over, and deprecate pageId across a later release.");

  // ─── C) One change, judged by all three modes ───────────────────────────────
  log("");
  log("═══ C) COMPATIBILITY MODES — same change, three verdicts, three upgrade orders ═══");
  const latest = reg.latest("ClickEvent")!;
  // Adding a REQUIRED field with no default: forward-OK (old reader ignores it),
  // but backward-BROKEN (new reader can't fill it from old data).
  const addRequired: Field[] = [...latest.fields, { name: "sessionId", type: "string", optional: false }];
  const { backward, forward } = compatReasons(latest.fields, addRequired);
  log("   proposed change: add REQUIRED field 'sessionId' (no default)");
  log(`   • under BACKWARD → ${backward.length === 0 ? "accept" : "✗ REJECT"} (${backward.length === 0 ? "—" : backward[0]})`);
  log(`   • under FORWARD  → ${forward.length === 0 ? "✓ ACCEPT — old consumers ignore the extra field; upgrade PRODUCERS first" : "reject"}`);
  log(`   • under FULL     → ${backward.length === 0 && forward.length === 0 ? "accept" : "✗ REJECT — FULL needs BOTH directions"}`);
  log("   mirror case: DROPPING a required field is backward-OK but forward-broken — the reverse skew.");

  log("");
  log("A schema registry + backward compatibility is what lets fifty teams ship");
  log("independently: an optional field ADD (with a default) is safe and needs no");
  log("coordinated deploy, while a RENAME or type-narrowing is a breaking change the");
  log("registry REJECTS at registration — so you handle it as a MIGRATION (add a new");
  log("optional field, backfill, deprecate the old one). Backward = upgrade consumers");
  log("first; forward = upgrade producers first; full = either order. Messages carry a");
  log("schema ID, not the full schema. It's the governance twin of idempotency and CDC.");
  process.exit(0);
}

main();
