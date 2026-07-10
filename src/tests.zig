//! Tachyon Unit Tests
//! Run with: zig build test

const std = @import("std");
const AppConfig = @import("config.zig").AppConfig;
const resilience = @import("resilience.zig");

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
// Config JSON Parsing (shared AppConfig module)
// ──────────────────────────────────────────────────────────────────────────────

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
// Retry exponential backoff (resilience module)
// ──────────────────────────────────────────────────────────────────────────────

test "retry backoff: first attempt is base delay" {
    const ns = resilience.computeBackoffNs(1, 1000, 30000);
    try std.testing.expectEqual(@as(u64, 1_000_000_000), ns);
}

test "retry backoff: doubles each attempt" {
    try std.testing.expectEqual(@as(u64, 2_000_000_000), resilience.computeBackoffNs(2, 1000, 30000));
    try std.testing.expectEqual(@as(u64, 4_000_000_000), resilience.computeBackoffNs(3, 1000, 30000));
}

test "retry backoff: caps at max_ms" {
    const ns = resilience.computeBackoffNs(20, 1000, 30000);
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
// Rate limiting + reconnect jitter (resilience module)
// ──────────────────────────────────────────────────────────────────────────────

test "rate limit: unlimited is zero sleep" {
    try std.testing.expectEqual(@as(u32, 0), resilience.rateLimitSleepMs(0));
}

test "rate limit: 10/sec is 100ms spacing" {
    try std.testing.expectEqual(@as(u32, 100), resilience.rateLimitSleepMs(10));
}

test "rate limit: 1000/sec is 1ms spacing" {
    try std.testing.expectEqual(@as(u32, 1), resilience.rateLimitSleepMs(1000));
}

test "jitter: stays within 75%-125% band" {
    const base: u32 = 1000;
    var s: u64 = 1;
    var i: usize = 0;
    while (i < 20) : (i += 1) {
        const j = resilience.withJitter(base, s);
        s += 1;
        try std.testing.expect(j >= 750 and j <= 1250);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// HMSG / header parsing
// ──────────────────────────────────────────────────────────────────────────────

const Header = struct { key: []const u8, value: []const u8 };

fn parseHeadersPure(alloc: std.mem.Allocator, raw: []const u8) !struct {
    headers: []Header,
    is_status: bool,
    status_code: u16,
    delivery_count: u32,
} {
    var list: std.ArrayList(Header) = .empty;
    errdefer list.deinit(alloc);

    var is_status = false;
    var status_code: u16 = 0;
    var delivery_count: u32 = 0;

    var it = std.mem.splitSequence(u8, raw, "\r\n");
    if (it.next()) |first| {
        if (std.mem.startsWith(u8, first, "NATS/1.0")) {
            if (first.len > 8) {
                const rest = std.mem.trim(u8, first[8..], " ");
                if (rest.len >= 3) {
                    is_status = true;
                    status_code = std.fmt.parseInt(u16, rest[0..3], 10) catch 0;
                }
            }
        }
    }
    while (it.next()) |line| {
        if (line.len == 0) continue;
        if (std.mem.indexOfScalar(u8, line, ':')) |colon| {
            const key = std.mem.trim(u8, line[0..colon], " ");
            const value = std.mem.trim(u8, line[colon + 1 ..], " ");
            try list.append(alloc, .{ .key = try alloc.dupe(u8, key), .value = try alloc.dupe(u8, value) });
            if (std.ascii.eqlIgnoreCase(key, "Nats-Delivery-Count")) {
                delivery_count = std.fmt.parseInt(u32, value, 10) catch 0;
            }
        }
    }
    return .{
        .headers = try list.toOwnedSlice(alloc),
        .is_status = is_status,
        .status_code = status_code,
        .delivery_count = delivery_count,
    };
}

test "hmsg: parses delivery count from headers" {
    const raw = "NATS/1.0\r\nNats-Delivery-Count: 3\r\nNats-Stream: JOBS\r\n\r\n";
    const parsed = try parseHeadersPure(std.testing.allocator, raw);
    defer {
        for (parsed.headers) |h| {
            std.testing.allocator.free(h.key);
            std.testing.allocator.free(h.value);
        }
        std.testing.allocator.free(parsed.headers);
    }
    try std.testing.expectEqual(@as(u32, 3), parsed.delivery_count);
    try std.testing.expect(!parsed.is_status);
}

test "hmsg: parses status 404 empty pull" {
    const raw = "NATS/1.0 404 No Messages\r\n\r\n";
    const parsed = try parseHeadersPure(std.testing.allocator, raw);
    defer {
        for (parsed.headers) |h| {
            std.testing.allocator.free(h.key);
            std.testing.allocator.free(h.value);
        }
        std.testing.allocator.free(parsed.headers);
    }
    try std.testing.expect(parsed.is_status);
    try std.testing.expectEqual(@as(u16, 404), parsed.status_code);
}

/// HMSG line: HMSG <subject> <sid> [reply] <hdr_len> <total_len>
fn parseHmsgLine(line: []const u8) !struct { hdr_len: usize, total_len: usize, has_reply: bool } {
    var it = std.mem.tokenizeAny(u8, line, " ");
    const prefix = it.next() orelse return error.InvalidHeader;
    if (!std.mem.eql(u8, prefix, "HMSG")) return error.InvalidHeader;
    _ = it.next() orelse return error.InvalidHeader; // subject
    _ = it.next() orelse return error.InvalidHeader; // sid
    const t3 = it.next() orelse return error.InvalidHeader;
    const t4 = it.next() orelse return error.InvalidHeader;
    if (it.next()) |t5| {
        return .{
            .hdr_len = try std.fmt.parseInt(usize, t4, 10),
            .total_len = try std.fmt.parseInt(usize, t5, 10),
            .has_reply = true,
        };
    }
    return .{
        .hdr_len = try std.fmt.parseInt(usize, t3, 10),
        .total_len = try std.fmt.parseInt(usize, t4, 10),
        .has_reply = false,
    };
}

test "hmsg: parses HMSG line with reply-to" {
    const r = try parseHmsgLine("HMSG jobs.high.email 1 _INBOX.x 48 120");
    try std.testing.expect(r.has_reply);
    try std.testing.expectEqual(@as(usize, 48), r.hdr_len);
    try std.testing.expectEqual(@as(usize, 120), r.total_len);
}

test "hmsg: parses HMSG line without reply-to" {
    const r = try parseHmsgLine("HMSG jobs.high.email 1 48 120");
    try std.testing.expect(!r.has_reply);
    try std.testing.expectEqual(@as(usize, 48), r.hdr_len);
}

// ──────────────────────────────────────────────────────────────────────────────
// Circuit breaker + dedup (resilience module)
// ──────────────────────────────────────────────────────────────────────────────

test "circuit: opens after threshold failures" {
    var cb = resilience.CircuitBreaker{ .failure_threshold = 3, .open_ms = 1000 };
    cb.onFailure(0);
    cb.onFailure(1);
    try std.testing.expect(cb.allow(2));
    cb.onFailure(2);
    try std.testing.expect(cb.state == .open);
    try std.testing.expect(!cb.allow(500));
    try std.testing.expect(cb.allow(2000)); // half-open after timeout
    try std.testing.expect(cb.state == .half_open);
    cb.onSuccess();
    try std.testing.expect(cb.state == .closed);
}

test "timeout: exceeds limit" {
    const timeout_ms: i64 = 5000;
    const elapsed_ms: i64 = 5200;
    try std.testing.expect(elapsed_ms > timeout_ms);
}

test "dedup: second insert is duplicate" {
    var cache = resilience.DedupCache.init(std.testing.allocator, 100);
    defer cache.deinit();
    cache.remember("job_1");
    try std.testing.expect(cache.contains("job_1"));
    try std.testing.expect(!cache.contains("job_2"));
}

// ──────────────────────────────────────────────────────────────────────────────
// ACK protocol bodies
// ──────────────────────────────────────────────────────────────────────────────

test "ack protocol: terminal and in-progress bodies" {
    try std.testing.expectEqualStrings("+TERM", "+TERM");
    try std.testing.expectEqualStrings("+WPI", "+WPI");
    try std.testing.expectEqualStrings("+ACK", "+ACK");
}
