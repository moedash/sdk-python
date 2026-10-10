# Benchmark results

Local runs on one machine, one client process. They give a starting point,
not production numbers. Redis runs on the same host as the dev server and
the client.

Machine: Apple M4 Max, 14 cores, Python 3.13.6, Redis 8.10.1, the Temporal dev server.

Commands, with `<port>` the Redis of each persistence mode:

```
uv run python -m tests.contrib.streams.bench.run_bench perf04-activity --redis redis://localhost:<port>/0 --count 300
uv run python -m tests.contrib.streams.bench.run_bench perf04-workflow --redis redis://localhost:<port>/0 --count 200
uv run python -m tests.contrib.streams.bench.run_bench perf05a --redis redis://localhost:<port>/0 --duration 10 --rate 2000
```

| Scenario | Redis | First append ms | p50 ms | p95 ms | p99 ms | Records/s | Behind schedule ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| perf04-activity | no persistence | 4.05 | 1.55 | 3.17 | 3.94 |  |  |
| perf04-activity | AOF, `appendfsync everysec` | 4.67 | 2.19 | 3.68 | 5.0 |  |  |
| perf04-activity | AOF, `appendfsync always` | 8.95 | 6.61 | 8.89 | 15.29 |  |  |
| perf04-workflow | no persistence |  | 10.62 | 16.96 | 17.83 |  |  |
| perf04-workflow | AOF, `appendfsync everysec` |  | 13.86 | 17.29 | 19.17 |  |  |
| perf04-workflow | AOF, `appendfsync always` |  | 24.47 | 28.13 | 29.55 |  |  |
| perf05a | no persistence |  | 2.56 | 3.31 | 4.7 | 2000.4 | 10.05 |
| perf05a | AOF, `appendfsync everysec` |  | 2.55 | 3.3 | 4.6 | 2000.4 | 9.76 |
| perf05a | AOF, `appendfsync always` |  | 1092.55 | 2129.13 | 2220.11 | 1554.6 | 2226.29 |

PERF-05a sends batches of 10 records at a target of 2,000 records a second.
With `appendfsync always`, every append waits for an fsync, and the
producer can't keep the 200 appends a second this target needs: it fell
more than two seconds behind, and the latency includes that wait. The
other two modes keep up.

## Raw output

perf04-activity, no persistence:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-activity", "gap_ms": 20, "first_append_ms": 4.05, "count": 299, "p50_ms": 1.55, "p95_ms": 3.17, "p99_ms": 3.94, "max_ms": 4.31, "mean_ms": 1.71}}
```

perf04-activity, AOF, `appendfsync everysec`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-activity", "gap_ms": 20, "first_append_ms": 4.67, "count": 299, "p50_ms": 2.19, "p95_ms": 3.68, "p99_ms": 5.0, "max_ms": 7.1, "mean_ms": 2.12}}
```

perf04-activity, AOF, `appendfsync always`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-activity", "gap_ms": 20, "first_append_ms": 8.95, "count": 299, "p50_ms": 6.61, "p95_ms": 8.89, "p99_ms": 15.29, "max_ms": 20.05, "mean_ms": 6.73}}
```

perf04-workflow, no persistence:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-workflow", "gap_ms": 20, "count": 200, "p50_ms": 10.62, "p95_ms": 16.96, "p99_ms": 17.83, "max_ms": 19.83, "mean_ms": 11.51, "history_reads": 200, "history_reads_per_publish": 1.0, "history_read": {"count": 200, "p50_ms": 6.43, "p95_ms": 10.74, "p99_ms": 11.31, "max_ms": 13.39, "mean_ms": 6.9}}}
```

perf04-workflow, AOF, `appendfsync everysec`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-workflow", "gap_ms": 20, "count": 200, "p50_ms": 13.86, "p95_ms": 17.29, "p99_ms": 19.17, "max_ms": 19.31, "mean_ms": 13.55, "history_reads": 200, "history_reads_per_publish": 1.0, "history_read": {"count": 200, "p50_ms": 8.55, "p95_ms": 10.9, "p99_ms": 13.12, "max_ms": 13.6, "mean_ms": 8.13}}}
```

perf04-workflow, AOF, `appendfsync always`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-workflow", "gap_ms": 20, "count": 200, "p50_ms": 24.47, "p95_ms": 28.13, "p99_ms": 29.55, "max_ms": 30.19, "mean_ms": 23.33, "history_reads": 200, "history_reads_per_publish": 1.0, "history_read": {"count": 200, "p50_ms": 9.58, "p95_ms": 11.43, "p99_ms": 12.25, "max_ms": 14.46, "mean_ms": 8.32}}}
```

perf05a, no persistence:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf05a", "streams": 1, "readers_per_stream": 1, "record_bytes": 100, "batch": 10, "target_rate_per_stream": 2000.0, "max_behind_schedule_ms": 10.05, "sent_records": 20010, "delivered": 20010, "expected_deliveries": 20010, "complete": true, "published_records_per_s": 2001.0, "delivered_records_per_s": 2000.4, "delivered_bytes_per_s": 200035.2, "count": 20010, "p50_ms": 2.56, "p95_ms": 3.31, "p99_ms": 4.7, "max_ms": 14.87, "mean_ms": 2.55}}
```

perf05a, AOF, `appendfsync everysec`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf05a", "streams": 1, "readers_per_stream": 1, "record_bytes": 100, "batch": 10, "target_rate_per_stream": 2000.0, "max_behind_schedule_ms": 9.76, "sent_records": 20010, "delivered": 20010, "expected_deliveries": 20010, "complete": true, "published_records_per_s": 2001.0, "delivered_records_per_s": 2000.4, "delivered_bytes_per_s": 200038.3, "count": 20010, "p50_ms": 2.55, "p95_ms": 3.3, "p99_ms": 4.6, "max_ms": 14.51, "mean_ms": 2.55}}
```

perf05a, AOF, `appendfsync always`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf05a", "streams": 1, "readers_per_stream": 1, "record_bytes": 100, "batch": 10, "target_rate_per_stream": 2000.0, "max_behind_schedule_ms": 2226.29, "sent_records": 15550, "delivered": 15550, "expected_deliveries": 15550, "complete": true, "published_records_per_s": 1555.0, "delivered_records_per_s": 1554.6, "delivered_bytes_per_s": 155462.2, "count": 15550, "p50_ms": 1092.55, "p95_ms": 2129.13, "p99_ms": 2220.11, "max_ms": 2232.12, "mean_ms": 1084.97}}
```
