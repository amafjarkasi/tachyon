# Contributing to Tachyon

Thank you for your interest in contributing! This document explains how to get involved.

---

## Code of Conduct

Be respectful and professional. Harassment of any kind is not tolerated.

---

## How to Contribute

### 1. Report a Bug

Open a [GitHub Issue](https://github.com/amafjarkasi/tachyon/issues) with:
- Your OS and Zig version (`zig version`)
- The exact steps to reproduce the issue
- Expected vs. actual behavior
- Any relevant logs or stack traces

### 2. Suggest a Feature

Open a GitHub Issue with the `enhancement` label. Describe:
- What problem you are trying to solve
- Your proposed solution or API shape
- Any tradeoffs or alternatives you considered

### 3. Submit a Pull Request

1. **Fork** the repository and clone your fork locally.
2. **Create a branch** from `master`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes.** Keep commits small and focused.
4. **Run the build** to verify nothing is broken:
   ```bash
   zig build -Doptimize=ReleaseFast
   ```
5. **Run tests** (when available):
   ```bash
   zig build test
   ```
6. **Push** your branch and open a Pull Request against `master`.
7. Fill in the PR template and link any related issues.

---

## Development Setup

### Prerequisites
- [Zig 0.16.0](https://ziglang.org/download/) — must be on `PATH`
- [NATS Server](https://nats.io/download/) — for local integration testing

### Running Locally

```bash
# Start NATS with JetStream enabled
nats-server -js

# Build all binaries
zig build -Doptimize=ReleaseFast

# Run the worker pool (4 threads, batch size 100)
zig build run-worker -Doptimize=ReleaseFast -- --threads 4 --batch 100

# Enqueue a single test job
zig build run-producer -Doptimize=ReleaseFast

# Run the benchmark stress test
zig build run-benchmark-producer -Doptimize=ReleaseFast -- --jobs 50000
```

---

## Code Style

- Follow the existing Zig formatting conventions
- Run `zig fmt src/` before committing
- Keep functions focused and small
- Add a comment explaining *why* for any non-obvious logic
- Avoid unnecessary heap allocations on hot paths

---

## Commit Message Format

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add retry-with-backoff to producer
fix: prevent worker deadlock on empty queue
docs: add DLQ troubleshooting section
perf: reduce arena allocations per batch
test: add nats_client MSG frame parsing tests
```

---

## Questions?

Open an issue with the `question` label or start a GitHub Discussion.
