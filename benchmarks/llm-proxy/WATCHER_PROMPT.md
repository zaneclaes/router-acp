Act as a CI watcher. Read `README.md`, then read `checks/01.json` through
`checks/20.json` in numeric order using one separate developer tool call per
file. Do not combine paths or use a loop.

After all snapshots are read, create `result.json` containing exactly these
semantic fields:

- `total`: number of snapshots
- `passed`: number whose status is `passed`
- `pending`: number whose status is `pending`
- `pending_services`: pending service names in snapshot order
- `max_duration_ms`: maximum observed duration

Do not modify any existing file. After writing `result.json`, read it once,
verify the counts against the snapshots, and finish with
`WATCHER_BENCHMARK_OK`.
