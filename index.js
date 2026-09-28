// aruba-support-bridge — reads the Hostinger support inbox and feeds each email to the support-triage
// edge function (which classifies, looks up the customer, diagnoses, drafts a reply, and files a ticket).
//
// This is intentionally a thin IMAP -> HTTP bridge: all the smarts live in the edge function. Run it once to
// chew through the backlog, or on a schedule for continuous triage.
//
// Env:
//   IMAP_HOST (default imap.hostinger.com) IMAP_PORT (993) IMAP_USER IMAP_PASS
//   TRIAGE_URL  e.g. https://kuudnizhwazeiwnmgmvz.supabase.co/functions/v1/support-triage
//   SUPABASE_ANON_KEY   (passes the platform verify_jwt gate)
//   SUPPORT_INGEST_SECRET   (must match the function's secret)
//   MODE   "recent" (last N, for testing) | "unseen" (production)   BATCH  how many (default 8)
//   MARK_SEEN  "1" to mark processed messages \Seen (production)

import { ImapFlow } from "imapflow";
import { simpleParser } from "mailparser";

const env = process.env;
const IMAP_HOST = env.IMAP_HOST || "imap.hostinger.com";
const IMAP_PORT = Number(env.IMAP_PORT || 993);
const BATCH = Number(env.BATCH || 8);
const MODE = env.MODE || "recent";
const MARK_SEEN = env.MARK_SEEN === "1";
// REPROCESS: re-run the (improved) matcher over already-filed emails and UPDATE their tickets in place,
// instead of skipping them as duplicates. Use once after deploying a matcher change to refresh the backlog.
const REPROCESS = env.REPROCESS === "1";
// SKIP_IF_REPLIED (default ON): before touching a customer email, check the Sent folder — if a reply already
// went out to that customer AT OR AFTER their email, the team (or the bot) already handled it, so skip it.
// This is what stops the bot ever double-replying on a thread a human is already working.
const SKIP_IF_REPLIED = env.SKIP_IF_REPLIED !== "0";
const SENT_MAILBOX = env.SENT_MAILBOX || "INBOX.Sent";
const SENT_SCAN = Number(env.SENT_SCAN || 1000);
// DRY_RUN: do everything EXCEPT actually call the triage — logs what it would do, sends nothing. For safe testing.
const DRY_RUN = env.DRY_RUN === "1";
// QUIET: suppress per-email log lines (from-address + subject = customer PII), keeping only counts/summary.
// Use when logs are viewable by others (e.g. a public repo's Actions logs) so no customer data leaks.
const QUIET = env.QUIET === "1";
const plog = (...a) => { if (!QUIET) console.log(...a); };
// Only send THIS domain's emails to the triage. The shared inbox holds mail for several Aruba domains; each
// domain's bot/CRM handles only its own (routed by the To: address). Set SUPPORT_SITE per domain.
const SUPPORT_SITE = env.SUPPORT_SITE || "arubaedcardexpress";
const siteFromTo = (to) => {
  const t = (to || "").toLowerCase();
  if (t.includes("arubaedcardexpress")) return "arubaedcardexpress";
  if (t.includes("edcardaruba")) return "edcardaruba";
  return "other";
};

function needed(...keys) {
  const miss = keys.filter((k) => !env[k]);
  if (miss.length) { console.error("Missing env:", miss.join(", ")); process.exit(1); }
}
needed("IMAP_USER", "IMAP_PASS", "TRIAGE_URL", "SUPABASE_ANON_KEY", "SUPPORT_INGEST_SECRET");

const addr = (a) => (a && a.value && a.value[0]) ? a.value[0].address : "";
const addrName = (a) => (a && a.value && a.value[0]) ? (a.value[0].name || "") : "";

async function triage(payload) {
  const r = await fetch(env.TRIAGE_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: env.SUPABASE_ANON_KEY,
      Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
      "x-support-secret": env.SUPPORT_INGEST_SECRET,
    },
    body: JSON.stringify(payload),
  });
  const text = await r.text();
  let body; try { body = JSON.parse(text); } catch { body = text; }
  return { status: r.status, body };
}

// Build a map of { recipientEmail -> latest time we sent them anything }, from the Sent folder.
// Used to skip customers who've already been replied to (by the team or the bot).
async function buildSentMap(client) {
  const map = new Map();
  const lock = await client.getMailboxLock(SENT_MAILBOX).catch(() => null);
  if (!lock) { console.log(`(no ${SENT_MAILBOX} mailbox — thread-reply check disabled)`); return map; }
  try {
    const status = await client.status(SENT_MAILBOX, { messages: true });
    const total = status.messages || 0;
    const first = Math.max(1, total - SENT_SCAN + 1);
    for await (const m of client.fetch(`${first}:*`, { envelope: true })) {
      const env2 = m.envelope; if (!env2) continue;
      const d = env2.date ? new Date(env2.date).getTime() : 0;
      for (const a of (env2.to || [])) {
        const e = (a.address || "").toLowerCase();
        if (!e) continue;
        if (!map.has(e) || map.get(e) < d) map.set(e, d);
      }
    }
  } finally { lock.release(); }
  console.log(`sent-folder scan: ${map.size} recipients (last ${SENT_SCAN} sent)`);
  return map;
}

async function main() {
  const client = new ImapFlow({ host: IMAP_HOST, port: IMAP_PORT, secure: true, auth: { user: env.IMAP_USER, pass: env.IMAP_PASS }, logger: false });
  await client.connect();
  console.log(`connected to ${IMAP_HOST} as ${env.IMAP_USER}`);
  const sentMap = SKIP_IF_REPLIED ? await buildSentMap(client) : new Map();
  const lock = await client.getMailboxLock("INBOX");
  let done = 0, made = 0, deduped = 0, failed = 0, skipped = 0, skippedTriage = 0, repliedSkip = 0;
  try {
    const status = await client.status("INBOX", { messages: true });
    const total = status.messages || 0;
    let uids;
    if (MODE === "unseen") {
      uids = await client.search({ seen: false }, { uid: true });
      uids = uids.slice(0, BATCH);
    } else {
      // last N by sequence -> resolve to uids
      const first = Math.max(1, total - BATCH + 1);
      uids = [];
      for await (const m of client.fetch(`${first}:*`, { uid: true })) uids.push(m.uid);
    }
    console.log(`INBOX total=${total}; processing ${uids.length} message(s) [mode=${MODE}]`);

    for (const uid of uids) {
      const msg = await client.fetchOne(uid, { source: true, envelope: true }, { uid: true }).catch(() => null);
      if (!msg || !msg.source) { failed++; continue; }
      let parsed; try { parsed = await simpleParser(msg.source); } catch { failed++; continue; }
      const payload = {
        messageId: parsed.messageId || String(msg.envelope?.messageId || `uid-${uid}`),
        from: addr(parsed.from), fromName: addrName(parsed.from),
        to: (parsed.to && parsed.to.text) || "",
        subject: parsed.subject || "",
        text: (parsed.text || parsed.html?.replace(/<[^>]+>/g, " ") || "").replace(/\s+\n/g, "\n").trim(),
        receivedAt: (parsed.date || new Date()).toISOString(),
        reprocess: REPROCESS,
      };
      const site = siteFromTo(payload.to);
      if (SUPPORT_SITE && site !== SUPPORT_SITE) { skipped++; continue; } // belongs to another domain's CRM
      // Already replied to (team or bot)? A sent message to this customer at/after their email = handled → skip.
      if (SKIP_IF_REPLIED && payload.from) {
        const sentAt = sentMap.get(payload.from.toLowerCase());
        const inAt = new Date(payload.receivedAt).getTime();
        if (sentAt && sentAt >= inAt - 60000) { repliedSkip++; plog(`  ~ already replied (team/bot) | ${payload.from.slice(0,24)} | ${payload.subject.slice(0,38)}`); continue; }
      }
      if (DRY_RUN) { made++; plog(`  → would process | ${payload.from.slice(0,24)} | ${payload.subject.slice(0,40)}`); continue; }
      const res = await triage(payload).catch((e) => ({ status: 0, body: e.message }));
      done++;
      if (res.status === 200 && res.body?.ok) {
        if (res.body.skipped) { skippedTriage++; plog(`  ~ skip [${res.body.skipped}]${res.body.removed ? " (removed stale)" : ""} | ${payload.from.slice(0,24)} | ${payload.subject.slice(0,38)}`); }
        else if (res.body.deduped) { deduped++; plog(`  · dup   ${payload.from.slice(0,28).padEnd(28)} | ${payload.subject.slice(0,40)}`); }
        else { made++; plog(`  ✓ ${res.body.category?.padEnd(14)} ${res.body.suggested_action?.padEnd(14)} matched=${res.body.matched} conf=${res.body.confidence ?? "-"}${res.body.match_reason ? " ("+res.body.match_reason+")" : ""} | ${payload.from.slice(0,24)} | ${payload.subject.slice(0,38)}`); }
        if (MARK_SEEN && !res.body.deduped && !res.body.skipped) await client.messageFlagsAdd(uid, ["\\Seen"], { uid: true }).catch(() => {});
      } else {
        failed++; plog(`  ✗ [${res.status}] ${JSON.stringify(res.body).slice(0,160)} | ${payload.subject.slice(0,40)}`);
      }
    }
  } finally {
    lock.release(); await client.logout();
  }
  console.log(`\nDONE site=${SUPPORT_SITE} processed=${done} tickets=${made} deduped=${deduped} already-replied=${repliedSkip} routed-away=${skippedTriage} skipped(other-domain)=${skipped} failed=${failed}`);
}
main().catch((e) => { console.error("FATAL", e); process.exit(1); });
