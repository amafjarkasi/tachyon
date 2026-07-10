const std = @import("std");

/// Default job payload shape used by producer / worker.
pub const Job = struct {
    id: []const u8,
    email: []const u8,
    subject: []const u8,
    body: []const u8,
};

/// Domain handler plug-in point.
/// Replace this body with SMTP / HTTP / DB work for your product.
/// Returns error.Timeout if wall time exceeds job_timeout_ms (soft deadline).
///
/// Keep the hot path quiet: per-job stdout logging destroys throughput.
pub fn processJob(job: Job, thread_id: usize, timeout_ms: u32, io: std.Io, progress: ?*const fn () void) !void {
    _ = thread_id;
    _ = job;
    const start = std.Io.Timestamp.now(io, .awake);

    if (progress) |p| p();

    // Soft timeout check (cooperative — hanging native calls still need JetStream ack_wait)
    if (timeout_ms > 0) {
        const elapsed = start.durationTo(std.Io.Timestamp.now(io, .awake)).toMilliseconds();
        if (elapsed > timeout_ms) return error.Timeout;
    }
}
