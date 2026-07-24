import assert from "node:assert/strict";
import test from "node:test";

import {
  beginLocalTurn,
  endLocalTurn,
  reconcileSessions,
} from "../src/busy.mjs";

test("local busy transitions touch only the selected session", () => {
  const original = [{ id: "a", busy: false }, { id: "b", busy: true }];
  const begun = beginLocalTurn(original, "a");
  assert.deepEqual(begun, [{ id: "a", busy: true }, { id: "b", busy: true }]);
  assert.deepEqual(endLocalTurn(begun, "a"), [
    { id: "a", busy: false },
    { id: "b", busy: true },
  ]);
  assert.equal(original[0].busy, false);
});

test("optimistic busy survives a stale relay snapshot", () => {
  const result = reconcileSessions(
    [{ id: "a", busy: true, title: "old" }],
    [{ id: "a", busy: false, title: "new" }, { id: "b", busy: false }],
    new Set(["a"]),
  );
  assert.deepEqual(result, [
    { id: "a", busy: true, title: "new" },
    { id: "b", busy: false },
  ]);
});
