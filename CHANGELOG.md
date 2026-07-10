# Changelog

All notable changes to Tachyon are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Changed
- Modular source layout: split `worker.zig` into `config`, `resilience`, `job`, `metrics_server`, and `logging` modules

---

## [0.2.0] - 2026-07-10

### Added
- NATS **HMSG** header support (`readMsg` parses headers; `publishWithHeaders` / HPUB)
- JetStream `Nats-Delivery-Count` used for real retry attempt numbers
- JetStream `nack()` with optional redelivery delay (`-NAK` / `-NAK {"delay":...}`)
- Job retry with exponential backoff (`retry_base_ms`, `retry_max_ms`) and consumer `max_deliver`
- Terminal ACK (`+TERM`) and in-progress ACK (`+WPI`) for progress / exhausted retries
- Buffered batch ACK (`ackBuffered` + `flushWrites`) to cut TCP round-trips
- Soft per-job timeout (`job_timeout_ms`) with NAK on exceed
- In-process job deduplication by `job.id` (bounded cache)
- Circuit breaker (opens after consecutive failures; half-open probe)
- JetStream-backed DLQ stream (`DEAD_LETTERS` / `jobs.failed`) auto-created at startup
- Real `processJob` handler (logs structured job fields; plug-in point for domain work)
- POSIX `SIGINT`/`SIGTERM` graceful shutdown via `std.posix.sigaction` (alongside Windows Ctrl+C)
- HTTP `/health` endpoint for Kubernetes liveness/readiness probes (alongside `/metrics`)
- Metrics: `zig_jobs_failed_total` counter
- Configurable stream, consumer, subject, and DLQ names
- Optional JetStream message TTL via stream `max_age` (`job_ttl_seconds`)
- Optional per-worker rate limiting (`max_jobs_per_second`)
- Reconnect backoff jitter (±25%) to reduce thundering-herd reconnects
- Per-job SLA latency alerting (`warn` log when job exceeds 500ms)
- Structured JSON logging
- Environment variable config overrides (NATS + stream/retry/rate/timeout keys)
- Full CLI flag reference (`--threads`, `--batch`, `--help`)
- Production Deployment Guide, CONTRIBUTING, SECURITY, Dockerfile, CI

---

## [0.1.0] - 2026-07-09

### Added
- Initial release of Tachyon — zero-dependency background job processor in Zig 0.16.0
- NATS JetStream integration via raw TCP/TLS socket (`nats_client.zig`)
- Multi-threaded worker pool with per-thread socket isolation (`worker.zig`)
- Elastic runtime auto-scaling (dynamic thread spawn/drain based on throughput)
- Zero-allocation arena reuse (`arena.reset(.retain_capacity)`)
- Adaptive batching backpressure control (latency-based batch size throttling)
- Precedence-aware configuration (CLI > env > `config.json` > defaults)
- Dual-priority JetStream queue routing (`WORKER_HIGH` / `WORKER_LOW`)
- Dead Letter Queue routing to `jobs.failed` subject
- Prometheus-compatible HTTP metrics server (`/metrics` on port 8080)
- Secure TLS transport with CA certificate bundle validation
- NATS authentication (username/password via CONNECT handshake)
- `producer.zig` — single-job enqueuer binary
- `benchmark_producer.zig` — high-throughput stress test producer (80/20 split)
- MIT License
