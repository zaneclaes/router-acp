import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";
import path from "node:path";

const root = process.argv[2];
if (!root) throw new Error("usage: node state_machine_quality_gate.mjs ROOT");
const api = await import(pathToFileURL(path.join(root, "src/index.mjs")));

let checks = 0;
const check = (condition, message) => {
  checks += 1;
  assert.ok(condition, message);
};

const sessions = Object.freeze([
  Object.freeze({ id: "a", busy: false, marker: 1 }),
  Object.freeze({ id: "b", busy: false, marker: 2 }),
]);
const begun = api.beginLocalTurn(sessions, "a");
check(begun !== sessions, "busy transition must copy");
check(begun[0].busy && !begun[1].busy, "wrong busy session");
check(api.endLocalTurn(begun, "a")[0].busy === false, "busy did not clear");

const reconciled = api.reconcileSessions(
  begun,
  [{ id: "a", busy: false, marker: 3 }, { id: "c", busy: false }],
  new Set(["a"]),
);
check(reconciled[0].busy === true, "stale snapshot clobbered optimistic busy");
check(reconciled[0].marker === 3, "fresh relay fields were lost");

const base = {
  activeSessionId: "tmp",
  sessions: [{ id: "tmp", pending: true, busy: false }],
  messages: [],
  pendingSend: null,
};
const attachment = Object.freeze({ id: "x", metadata: Object.freeze({ n: 1 }) });
let state = api.queuePendingSend(base, "tmp", "one", [attachment]);
state = api.queuePendingSend(state, "tmp", "two", [attachment]);
check(state.messages.length === 1, "requeue duplicated optimistic message");
check(state.pendingSend.text === "two", "requeue did not replace payload");
state = api.promotePendingSession(state, "tmp", {
  id: "real",
  pending: false,
  busy: false,
});
check(state.activeSessionId === "real", "active id not promoted");
check(state.messages.every((m) => m.sessionId === "real"), "message id not promoted");
const drained = api.drainPendingSend(state);
check(drained.request?.text === "two", "ready send did not drain");
check(drained.request?.attachments[0].metadata.n === 1, "attachment changed");
check(api.drainPendingSend(drained.state).request === null, "send drained twice");

const existing = Object.freeze([
  Object.freeze({ id: "m", createdAt: 4, revision: 4, content: "new" }),
]);
const merged = api.mergeHistory(existing, [
  { id: "m", createdAt: 4, revision: 3, content: "old" },
  { id: "z", createdAt: 5, revision: 1, content: "z" },
  { id: "a", createdAt: 5, revision: 1, content: "a" },
]);
check(merged[0].content === "new", "history regressed");
check(merged.map((m) => m.id).join(",") === "m,a,z", "history order unstable");
check(existing[0].content === "new", "history input mutated");

console.log(`STATE_MACHINE_QUALITY_GATE_OK: ${checks} independent checks`);
