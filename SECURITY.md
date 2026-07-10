# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| 0.1.x   | ✅ Yes    |

---

## Reporting a Vulnerability

**Please do NOT open a public GitHub Issue for security vulnerabilities.**

If you discover a security vulnerability in Tachyon — especially one involving:
- NATS credential handling or authentication bypass
- TLS certificate validation failures
- Arbitrary code execution via malformed payloads
- Information disclosure via the metrics endpoint (`/metrics`)
- Memory safety issues (buffer overflows, use-after-free)

Please report it **privately** by:

1. Opening a [GitHub Security Advisory](https://github.com/amafjarkasi/tachyon/security/advisories/new) (preferred)
2. Or emailing the maintainer directly via the email listed on the GitHub profile

---

## What to Include in Your Report

- A description of the vulnerability and its potential impact
- Steps to reproduce the issue
- Your Zig version and OS
- Any relevant logs, stack traces, or proof-of-concept code

---

## Response Timeline

- **Acknowledgment:** Within 72 hours of receiving the report
- **Initial assessment:** Within 7 days
- **Patch and disclosure:** Within 30 days for confirmed vulnerabilities

---

## Scope

The following are **in scope** for security reports:
- `src/nats_client.zig` — TLS handshake, credential serialization, MSG frame parsing
- `src/worker.zig` — payload deserialization, DLQ routing, metrics endpoint

The following are **out of scope**:
- Vulnerabilities in the NATS server itself (report to [nats-io/nats-server](https://github.com/nats-io/nats-server))
- Issues requiring physical access to the host machine
- Denial-of-service via exhausting NATS server resources

---

## Security Considerations for Deployment

- **Never commit `config.json` with real credentials** — it is gitignored by default
- **Enable TLS** (`"nats_tls": true`) in any non-localhost deployment
- **Restrict metrics port** (8080) — it is unauthenticated by design and should not be publicly exposed
- **Use NATS authentication** — set `NATS_USER` and `NATS_PASS` via environment variables in production
