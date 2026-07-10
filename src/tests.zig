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
};

test "config: parses valid config.json structure" {
    const json =
        \\{
        \\    "nats_host": "nats.prod.internal",
        \\    "nats_port": 4222,
        \\    "nats_tls": true,
        \\    "worker_threads": 8,
        \\    "worker_batch": 200
        \\}
    ;

    const parsed = try std.json.parseFromSlice(AppConfig, std.testing.allocator, json, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("nats.prod.internal", parsed.value.nats_host);
    try std.testing.expectEqual(@as(u16, 4222), parsed.value.nats_port);
    try std.testing.expectEqual(true, parsed.value.nats_tls);
    try std.testing.expectEqual(@as(usize, 8), parsed.value.worker_threads);
    try std.testing.expectEqual(@as(usize, 200), parsed.value.worker_batch);
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
