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
        if (self.state == .closed and self.consecutive_failures == 0) return;
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

/// FNV-1a 64-bit hash for job ids (no allocation).
pub fn hashId(id: []const u8) u64 {
    var h: u64 = 0xcbf29ce484222325;
    for (id) |c| {
        h ^= c;
        h *%= 0x100000001b3;
    }
    return h;
}

/// Bounded job-id dedup via open-addressed hash set of u64 hashes.
/// `max_size == 0` disables dedup. When full, oldest slots are overwritten (ring-like).
/// False positives possible (hash collision) — acceptable for soft idempotency.
pub const DedupCache = struct {
    slots: []u64,
    /// 0 = empty slot sentinel (ids hashing to 0 use 1).
    len: usize = 0,
    insert_cursor: usize = 0,
    allocator: std.mem.Allocator,
    max_size: usize,

    pub fn init(allocator: std.mem.Allocator, max_size: usize) !DedupCache {
        if (max_size == 0) {
            return .{
                .slots = &[_]u64{},
                .allocator = allocator,
                .max_size = 0,
            };
        }
        // Power-of-two capacity for fast mask; at least max_size*2 for load factor.
        var cap: usize = 16;
        while (cap < max_size * 2) : (cap *= 2) {}
        const slots = try allocator.alloc(u64, cap);
        @memset(slots, 0);
        return .{
            .slots = slots,
            .allocator = allocator,
            .max_size = max_size,
        };
    }

    pub fn deinit(self: *DedupCache) void {
        if (self.slots.len > 0) {
            self.allocator.free(self.slots);
            self.slots = &[_]u64{};
        }
    }

    pub fn enabled(self: *const DedupCache) bool {
        return self.max_size > 0 and self.slots.len > 0;
    }

    fn norm(h: u64) u64 {
        return if (h == 0) 1 else h;
    }

    pub fn contains(self: *const DedupCache, id: []const u8) bool {
        if (!self.enabled()) return false;
        const h = norm(hashId(id));
        const mask = self.slots.len - 1;
        var i: usize = @intCast(h & mask);
        var probes: usize = 0;
        while (probes < self.slots.len) : (probes += 1) {
            const v = self.slots[i];
            if (v == 0) return false;
            if (v == h) return true;
            i = (i + 1) & mask;
        }
        return false;
    }

    pub fn remember(self: *DedupCache, id: []const u8) void {
        if (!self.enabled()) return;
        const h = norm(hashId(id));
        const mask = self.slots.len - 1;
        var i: usize = @intCast(h & mask);
        var probes: usize = 0;
        while (probes < self.slots.len) : (probes += 1) {
            const v = self.slots[i];
            if (v == 0) {
                self.slots[i] = h;
                self.len += 1;
                return;
            }
            if (v == h) return; // already present
            i = (i + 1) & mask;
        }
        // Table full of tombstones/values: overwrite ring cursor slot.
        const idx = self.insert_cursor & mask;
        self.slots[idx] = h;
        self.insert_cursor +%= 1;
    }
};
