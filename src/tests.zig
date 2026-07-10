//! Tachyon Unit Tests
//! Run with: zig build test

const std = @import("std");

// ──────────────────────────────────────────────────────────────────────────────
// Adaptive Batching Math
// ──────────────────────────────────────────────────────────────────────────────

test "adaptive batch: throttles down when avg latency > 200ms" {
    const default_batch: usize = 100;
    var adaptive_batch: usize = default_batch;
    const avg_latency: i64 = 250; // ms — above 200ms threshold

    if (avg_latency > 200) {
        adaptive_batch = @max(adaptive_batch / 2, 1);
    }

    try std.testing.expectEqual(@as(usize, 50), adaptive_batch);
}

test "adaptive batch: recovers when avg latency < 50ms" {
    const default_batch: usize = 100;
    var adaptive_batch: usize = 50; // already throttled
    const avg_latency: i64 = 30; // ms — below 50ms threshold

    if (avg_latency < 50) {
        adaptive_batch = @min(adaptive_batch + 10, default_batch);
    }

    try std.testing.expectEqual(@as(usize, 60), adaptive_batch);
}

test "adaptive batch: clamps at 1 minimum" {
    var adaptive_batch: usize = 1;
    const avg_latency: i64 = 500;

    if (avg_latency > 200) {
        adaptive_batch = @max(adaptive_batch / 2, 1);
    }

    try std.testing.expectEqual(@as(usize, 1), adaptive_batch);
}

test "adaptive batch: clamps at max batch size ceiling" {
    const default_batch: usize = 100;
    var adaptive_batch: usize = 95;
    const avg_latency: i64 = 10;

    if (avg_latency < 50) {
        adaptive_batch = @min(adaptive_batch + 10, default_batch);
    }

    try std.testing.expectEqual(@as(usize, 100), adaptive_batch); // clamped, not 105
}

// ──────────────────────────────────────────────────────────────────────────────
// Auto-Scaling Threshold Logic
// ──────────────────────────────────────────────────────────────────────────────

test "auto-scale: scales up when throughput > 30000 and below ceiling" {
    const max_threads: usize = 8;
    var active_threads: usize = 4;
    const diff: usize = 35000; // jobs this second

    if (diff > 30000 and active_threads < max_threads) {
        active_threads += 1;
    }

    try std.testing.expectEqual(@as(usize, 5), active_threads);
}

test "auto-scale: does NOT scale up when at ceiling" {
    const max_threads: usize = 8;
    var active_threads: usize = 8;
    const diff: usize = 50000;

    if (diff > 30000 and active_threads < max_threads) {
        active_threads += 1;
    }

    try std.testing.expectEqual(@as(usize, 8), active_threads); // unchanged
}

test "auto-scale: scales down when throughput < 5000 and above minimum" {
    const min_threads: usize = 4;
    var active_threads: usize = 6;
    const diff: usize = 2000;

    if (diff < 5000 and active_threads > min_threads) {
        active_threads -= 1;
    }

    try std.testing.expectEqual(@as(usize, 5), active_threads);
}

test "auto-scale: does NOT scale down below minimum" {
    const min_threads: usize = 4;
    var active_threads: usize = 4;
    const diff: usize = 100;

    if (diff < 5000 and active_threads > min_threads) {
        active_threads -= 1;
    }

    try std.testing.expectEqual(@as(usize, 4), active_threads); // unchanged
}

// ──────────────────────────────────────────────────────────────────────────────
// Config JSON Parsing
// ──────────────────────────────────────────────────────────────────────────────

const AppConfig = struct {
    nats_host: []const u8 = "127.0.0.1",
    nats_port: u16 = 4222,
    nats_user: ?[]const u8 = null,
    nats_pass: ?[]const u8 = null,
    nats_tls: bool = false,
    nats_ca_path: ?[]const u8 = null,
    worker_threads: usize = 4,
    worker_batch: usize = 50,
    stream_name: []const u8 = "JOBS",
    consumer_high: []const u8 = "WORKER_HIGH",
    consumer_low: []const u8 = "WORKER_LOW",
    subject_high: []const u8 = "jobs.high.*",
    subject_low: []const u8 = "jobs.low.*",
    dlq_subject: []const u8 = "jobs.failed",
    max_deliver: u32 = 5,
    retry_base_ms: u32 = 1000,
    retry_max_ms: u32 = 30000,
    job_ttl_seconds: u64 = 0,
    max_jobs_per_second: u32 = 0,
};

test "config: parses valid config.json structure" {
    const json =
        \\{
        \\    "nats_host": "nats.prod.internal",
        \\    "nats_port": 4222,
        \\    "nats_tls": true,
        \\    "worker_threads": 8,
        \\    "worker_batch": 200,
        \\    "stream_name": "ORDERS",
        \\    "max_deliver": 10,
        \\    "job_ttl_seconds": 86400,
        \\    "max_jobs_per_second": 100
        \\}
    ;

    const parsed = try std.json.parseFromSlice(AppConfig, std.testing.allocator, json, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("nats.prod.internal", parsed.value.nats_host);
    try std.testing.expectEqual(@as(u16, 4222), parsed.value.nats_port);
    try std.testing.expectEqual(true, parsed.value.nats_tls);
    try std.testing.expectEqual(@as(usize, 8), parsed.value.worker_threads);
    try std.testing.expectEqual(@as(usize, 200), parsed.value.worker_batch);
    try std.testing.expectEqualStrings("ORDERS", parsed.value.stream_name);
    try std.testing.expectEqual(@as(u32, 10), parsed.value.max_deliver);
    try std.testing.expectEqual(@as(u64, 86400), parsed.value.job_ttl_seconds);
    try std.testing.expectEqual(@as(u32, 100), parsed.value.max_jobs_per_second);
}

test "config: applies defaults for missing fields" {
    const json = "{}";
    const parsed = try std.json.parseFromSlice(AppConfig, std.testing.allocator, json, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("127.0.0.1", parsed.value.nats_host);
    try std.testing.expectEqual(@as(u16, 4222), parsed.value.nats_port);
    try std.testing.expectEqual(false, parsed.value.nats_tls);
    try std.testing.expectEqual(@as(usize, 4), parsed.value.worker_threads);
    try std.testing.expectEqual(@as(usize, 50), parsed.value.worker_batch);
    try std.testing.expectEqualStrings("JOBS", parsed.value.stream_name);
    try std.testing.expectEqual(@as(u32, 5), parsed.value.max_deliver);
    try std.testing.expectEqual(@as(u64, 0), parsed.value.job_ttl_seconds);
    try std.testing.expectEqual(@as(u32, 0), parsed.value.max_jobs_per_second);
}

// ──────────────────────────────────────────────────────────────────────────────
// NATS MSG Frame Parsing (pure logic, no network)
// ──────────────────────────────────────────────────────────────────────────────

/// Parses a NATS MSG header line: "MSG <subject> <sid> [reply] <bytes>"
/// Returns payload byte count or error.
fn parseMsgHeader(line: []const u8) !usize {
    var it = std.mem.splitScalar(u8, line, ' ');
    const prefix = it.next() orelse return error.InvalidHeader;
    if (!std.mem.eql(u8, prefix, "MSG")) return error.InvalidHeader;
    _ = it.next() orelse return error.InvalidHeader; // subject
    _ = it.next() orelse return error.InvalidHeader; // sid
    const fourth = it.next() orelse return error.InvalidHeader;
    // If there's a 5th token, fourth is reply-to and 5th is bytes
    if (it.next()) |bytes_str| {
        // fourth is reply-to, bytes_str is the byte count
        return try std.fmt.parseInt(usize, bytes_str, 10);
    } else {
        // No reply-to, fourth IS bytes
        return try std.fmt.parseInt(usize, fourth, 10);
    }
}

test "nats_client: parses 3-token MSG header (no reply-to)" {
    const line = "MSG jobs.high.email 1 47";
    const bytes = try parseMsgHeader(line);
    try std.testing.expectEqual(@as(usize, 47), bytes);
}

test "nats_client: parses 4-token MSG header (with reply-to)" {
    const line = "MSG jobs.high.email 1 _INBOX.reply.abc 47";
    const bytes = try parseMsgHeader(line);
    try std.testing.expectEqual(@as(usize, 47), bytes);
}

test "nats_client: rejects non-MSG prefix" {
    const line = "PUB jobs.high.email 47";
    try std.testing.expectError(error.InvalidHeader, parseMsgHeader(line));
}

// ──────────────────────────────────────────────────────────────────────────────
// SLA Alerting Threshold
// ──────────────────────────────────────────────────────────────────────────────

test "sla: detects violations above 500ms" {
    const sla_limit_ms: i64 = 500;
    const job_latency_ms: i64 = 823;
    const violated = job_latency_ms > sla_limit_ms;
    try std.testing.expect(violated);
}

test "sla: no violation below 500ms" {
    const sla_limit_ms: i64 = 500;
    const job_latency_ms: i64 = 312;
    const violated = job_latency_ms > sla_limit_ms;
    try std.testing.expect(!violated);
}

// ──────────────────────────────────────────────────────────────────────────────
// NAK / NACK formatting
// ──────────────────────────────────────────────────────────────────────────────

/// Builds the JetStream NAK body. delay_ns == null → plain "-NAK"
/// delay_ns set → `-NAK {"delay":<ns>}`
fn formatNak(buf: []u8, delay_ns: ?u64) ![]const u8 {
    if (delay_ns) |d| {
        return try std.fmt.bufPrint(buf, "-NAK {{\"delay\":{d}}}", .{d});
    }
    return try std.fmt.bufPrint(buf, "-NAK", .{});
}

test "nack: plain NAK body" {
    var buf: [64]u8 = undefined;
    const body = try formatNak(&buf, null);
    try std.testing.expectEqualStrings("-NAK", body);
}

test "nack: delayed NAK body includes nanoseconds" {
    var buf: [64]u8 = undefined;
    const body = try formatNak(&buf, 2_000_000_000); // 2 seconds
    try std.testing.expectEqualStrings("-NAK {\"delay\":2000000000}", body);
}

// ──────────────────────────────────────────────────────────────────────────────
// Retry exponential backoff
// ──────────────────────────────────────────────────────────────────────────────

fn computeBackoffNs(attempt: u32, base_ms: u32, max_ms: u32) u64 {
    var shift: u5 = 0;
    if (attempt > 1) shift = @intCast(@min(attempt - 1, 16));
    const raw: u64 = @as(u64, base_ms) << shift;
    const capped: u64 = @min(raw, @as(u64, max_ms));
    return capped * 1_000_000;
}

test "retry backoff: first attempt is base delay" {
    const ns = computeBackoffNs(1, 1000, 30000);
    try std.testing.expectEqual(@as(u64, 1_000_000_000), ns);
}

test "retry backoff: doubles each attempt" {
    try std.testing.expectEqual(@as(u64, 2_000_000_000), computeBackoffNs(2, 1000, 30000));
    try std.testing.expectEqual(@as(u64, 4_000_000_000), computeBackoffNs(3, 1000, 30000));
}

test "retry backoff: caps at max_ms" {
    const ns = computeBackoffNs(20, 1000, 30000);
    try std.testing.expectEqual(@as(u64, 30_000_000_000), ns);
}

// ──────────────────────────────────────────────────────────────────────────────
// HTTP path routing (/health, /metrics)
// ──────────────────────────────────────────────────────────────────────────────

fn routeHttpPath(request_line: []const u8) enum { metrics, health, not_found } {
    if (std.mem.indexOf(u8, request_line, " /health") != null) return .health;
    if (std.mem.indexOf(u8, request_line, " /metrics") != null) return .metrics;
    return .not_found;
}

test "http route: /health" {
    try std.testing.expect(routeHttpPath("GET /health HTTP/1.1") == .health);
}

test "http route: /metrics" {
    try std.testing.expect(routeHttpPath("GET /metrics HTTP/1.1") == .metrics);
}

test "http route: unknown" {
    try std.testing.expect(routeHttpPath("GET /favicon.ico HTTP/1.1") == .not_found);
}

// ──────────────────────────────────────────────────────────────────────────────
// Job TTL conversion
// ──────────────────────────────────────────────────────────────────────────────

test "ttl: seconds to nanoseconds" {
    const seconds: u64 = 86400; // 24h
    const ns = seconds * 1_000_000_000;
    try std.testing.expectEqual(@as(u64, 86_400_000_000_000), ns);
}

// ──────────────────────────────────────────────────────────────────────────────
// Rate limiting
// ──────────────────────────────────────────────────────────────────────────────

fn rateLimitSleepMs(max_jobs_per_second: u32) u32 {
    if (max_jobs_per_second == 0) return 0;
    return @max(1, 1000 / max_jobs_per_second);
}

test "rate limit: unlimited is zero sleep" {
    try std.testing.expectEqual(@as(u32, 0), rateLimitSleepMs(0));
}

test "rate limit: 10/sec is 100ms spacing" {
    try std.testing.expectEqual(@as(u32, 100), rateLimitSleepMs(10));
}

test "rate limit: 1000/sec is 1ms spacing" {
    try std.testing.expectEqual(@as(u32, 1), rateLimitSleepMs(1000));
}

// ──────────────────────────────────────────────────────────────────────────────
// Reconnect jitter
// ──────────────────────────────────────────────────────────────────────────────

fn withJitter(backoff_ms: u32, seed: u64) u32 {
    const span: u64 = 50;
    const r = (seed *% 1103515245 +% 12345) % (span + 1); // 0..50
    const pct: u64 = 75 + r; // 75..125
    const jittered: u64 = (@as(u64, backoff_ms) * pct) / 100;
    return @intCast(@min(jittered, 60_000));
}

test "jitter: stays within 75%-125% band" {
    const base: u32 = 1000;
    var s: u64 = 1;
    var i: usize = 0;
    while (i < 20) : (i += 1) {
        const j = withJitter(base, s);
        s += 1;
        try std.testing.expect(j >= 750 and j <= 1250);
    }
}
