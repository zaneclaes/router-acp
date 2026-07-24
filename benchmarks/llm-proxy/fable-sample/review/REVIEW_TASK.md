# Deployment graph review

Review the fixture repository without editing it. Write `review.json` with:

```json
{
  "verdict": "CHANGES_REQUESTED",
  "findings": [
    {
      "code": "stable finding code",
      "file": "relative path",
      "line": 1,
      "explanation": "concise evidence and impact"
    }
  ]
}
```

Find every violation of `docs/invariants.md`. Include blocking findings only,
sorted by `code`. Exact finding codes are named in the invariant document.
