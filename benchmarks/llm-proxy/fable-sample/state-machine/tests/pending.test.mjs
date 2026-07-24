import assert from "node:assert/strict";
import test from "node:test";

import {
  drainPendingSend,
  promotePendingSession,
  queuePendingSend,
} from "../src/pending.mjs";

const initial = {
  activeSessionId: "tmp-1",
  sessions: [{ id: "tmp-1", pending: true, busy: false }],
  messages: [],
  pendingSend: null,
};

test("queue preserves attachments and creates one optimistic message", () => {
  const attachments = [{ id: "f1", name: "trace.txt" }];
  const once = queuePendingSend(initial, "tmp-1", "first", attachments);
  const twice = queuePendingSend(once, "tmp-1", "replacement", attachments);
  assert.equal(twice.messages.length, 1);
  assert.equal(twice.messages[0].sessionId, "tmp-1");
  assert.equal(twice.pendingSend.text, "replacement");
  assert.deepEqual(twice.pendingSend.attachments, attachments);
  assert.equal(initial.messages.length, 0);
});

test("promotion remaps state and drain dispatches exactly once", () => {
  const queued = queuePendingSend(initial, "tmp-1", "hello", [{ id: "f1" }]);
  const promoted = promotePendingSession(queued, "tmp-1", {
    id: "real-9",
    pending: false,
    busy: false,
  });
  assert.equal(promoted.activeSessionId, "real-9");
  assert.equal(promoted.messages[0].sessionId, "real-9");
  assert.equal(promoted.pendingSend.sessionId, "real-9");

  const first = drainPendingSend(promoted);
  assert.deepEqual(first.request, {
    sessionId: "real-9",
    text: "hello",
    attachments: [{ id: "f1" }],
  });
  assert.equal(first.state.pendingSend.dispatched, true);
  assert.equal(drainPendingSend(first.state).request, null);
});

test("drain waits while pending or busy", () => {
  const queued = queuePendingSend(initial, "tmp-1", "hello", []);
  assert.equal(drainPendingSend(queued).request, null);
  const promoted = promotePendingSession(queued, "tmp-1", {
    id: "real-9",
    pending: false,
    busy: true,
  });
  assert.equal(drainPendingSend(promoted).request, null);
});
