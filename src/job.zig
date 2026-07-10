const std = @import("std");
const logging = @import("logging.zig");

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
pub fn processJob(job: Job, thread_id: usize, timeout_ms: u32, io: std.Io, progress: ?*const fn () void) !void {
    const start = std.Io.Timestamp.now(io, .awake);

    var msg_buf: [160]u8 = undefined;
    const msg = std.fmt.bufPrint(&msg_buf, "Processing job id={s} to={s} subject={s}", .{ job.id, job.email, job.subject }) catch "Processing job";
    logging.logJSON("info", thread_id, msg);

    if (progress) |p| p();

    // Soft timeout check (cooperative — hanging native calls still need JetStream ack_wait)
    if (timeout_ms > 0) {
        const elapsed = start.durationTo(std.Io.Timestamp.now(io, .awake)).toMilliseconds();
        if (elapsed > timeout_ms) return error.Timeout;
    }
}
