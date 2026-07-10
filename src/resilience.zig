const std = @import("std");

/// Exponential backoff delay in nanoseconds for JetStream NAK.
/// attempt is 1-based: delay = min(base_ms * 2^(attempt-1), max_ms) converted to ns.
pub fn computeBackoffNs(attempt: u32, base_ms: u32, max_ms: u32) u64 {
    var shift: u5 = 0;
    if (attempt > 1) shift = @intCast(@min(attempt - 1, 16));
    const raw: u64 = @as(u64, base_ms) << shift;
    const capped: u64 = @min(raw, @as(u64, max_ms));
    return capped * 1_000_000;
}

/// Minimum sleep between jobs to enforce max_jobs_per_second. 0 means unlimited.
pub fn rateLimitSleepMs(max_jobs_per_second: u32) u32 {
    if (max_jobs_per_second == 0) return 0;
    return @max(1, 1000 / max_jobs_per_second);
}

/// Adds ±25% jitter to backoff_ms using a simple LCG seed.
pub fn withJitter(backoff_ms: u32, seed: u64) u32 {
    const span: u64 = 50;
    const r = (seed *% 1103515245 +% 12345) % (span + 1); // 0..50
    const pct: u64 = 75 + r; // 75..125
    const jittered: u64 = (@as(u64, backoff_ms) * pct) / 100;
    return @intCast(@min(jittered, 60_000));
}

pub const CircuitState = enum { closed, open, half_open };

pub const CircuitBreaker = struct {
    state: CircuitState = .closed,
    consecutive_failures: u32 = 0,
    open_until_ms: i64 = 0,
    failure_threshold: u32,
    open_ms: u32,

    pub fn allow(self: *CircuitBreaker, now_ms: i64) bool {
        return switch (self.state) {
            .closed => true,
            .half_open => true,
            .open => {
                if (now_ms >= self.open_until_ms) {
                    self.state = .half_open;
                    return true;
                }
                return false;
            },
        };
    }

    pub fn onSuccess(self: *CircuitBreaker) void {
        self.consecutive_failures = 0;
        self.state = .closed;
    }

    pub fn onFailure(self: *CircuitBreaker, now_ms: i64) void {
        self.consecutive_failures += 1;
        if (self.consecutive_failures >= self.failure_threshold) {
            self.state = .open;
            self.open_until_ms = now_ms + @as(i64, @intCast(self.open_ms));
        } else if (self.state == .half_open) {
            self.state = .open;
            self.open_until_ms = now_ms + @as(i64, @intCast(self.open_ms));
        }
    }
};

/// Bounded job-id dedup cache. Keys are owned by the map (allocator-backed).
pub const DedupCache = struct {
    map: std.StringHashMap(void),
    allocator: std.mem.Allocator,
    max_size: usize,

    pub fn init(allocator: std.mem.Allocator, max_size: usize) DedupCache {
        return .{
            .map = std.StringHashMap(void).init(allocator),
            .allocator = allocator,
            .max_size = max_size,
        };
    }

    pub fn deinit(self: *DedupCache) void {
        var it = self.map.keyIterator();
        while (it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.map.deinit();
    }

    pub fn contains(self: *const DedupCache, id: []const u8) bool {
        return self.map.contains(id);
    }

    pub fn remember(self: *DedupCache, id: []const u8) void {
        if (self.map.count() >= self.max_size) {
            var it = self.map.keyIterator();
            while (it.next()) |k| {
                self.allocator.free(k.*);
            }
            self.map.clearRetainingCapacity();
        }
        if (self.allocator.dupe(u8, id)) |id_copy| {
            self.map.put(id_copy, {}) catch {
                self.allocator.free(id_copy);
            };
        } else |_| {}
    }
};
