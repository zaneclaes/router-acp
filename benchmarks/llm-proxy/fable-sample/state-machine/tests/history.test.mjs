import assert from "node:assert/strict";
import test from "node:test";

import { mergeHistory } from "../src/history.mjs";

test("newer partial content replaces the same message", () => {
  const old = [
    { id: "u1", createdAt: 1, revision: 1, content: "question" },
    { id: "a1", createdAt: 2, revision: 1, content: "par" },
  ];
  const next = [
    { id: "a1", createdAt: 2, revision: 2, content: "partial answer" },
  ];
  assert.deepEqual(mergeHistory(old, next), [
    { id: "u1", createdAt: 1, revision: 1, content: "question" },
    { id: "a1", createdAt: 2, revision: 2, content: "partial answer" },
  ]);
  assert.equal(old[1].content, "par");
});

test("older snapshots do not regress and new ids sort stably", () => {
  const result = mergeHistory(
    [{ id: "b", createdAt: 2, revision: 3, content: "complete" }],
    [
      { id: "b", createdAt: 2, revision: 2, content: "partial" },
      { id: "c", createdAt: 3, revision: 1, content: "later" },
      { id: "a", createdAt: 2, revision: 1, content: "peer" },
    ],
  );
  assert.deepEqual(result.map((item) => item.id), ["a", "b", "c"]);
  assert.equal(result[1].content, "complete");
});
