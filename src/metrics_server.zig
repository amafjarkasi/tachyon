const std = @import("std");
const logging = @import("logging.zig");

pub const MetricsContext = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    total_jobs: *std.atomic.Value(usize),
    failed_jobs: *std.atomic.Value(usize),
    should_shutdown: *std.atomic.Value(bool),
};

/// HTTP server on 127.0.0.1:8080 serving /health and /metrics.
pub fn run(ctx: *MetricsContext) void {
    defer ctx.allocator.destroy(ctx);

    const addr = std.Io.net.IpAddress.parse("127.0.0.1", 8080) catch |err| {
        var err_buf: [64]u8 = undefined;
        const err_msg = std.fmt.bufPrint(&err_buf, "Metrics Address parse error: {}", .{err}) catch "Metrics parse error.";
        logging.logJSON("error", null, err_msg);
        return;
    };
    var server = addr.listen(ctx.io, .{ .reuse_address = true }) catch |err| {
        var err_buf: [64]u8 = undefined;
        const err_msg = std.fmt.bufPrint(&err_buf, "Metrics Listen error: {}", .{err}) catch "Metrics listen error.";
        logging.logJSON("error", null, err_msg);
        return;
    };

    logging.logJSON("info", null, "Metrics Server listening on http://127.0.0.1:8080 (/metrics, /health)");

    while (!ctx.should_shutdown.load(.monotonic)) {
        var conn = server.accept(ctx.io) catch continue;
        defer conn.close(ctx.io);

        var read_buf: [1024]u8 = undefined;
        var r = conn.reader(ctx.io, &read_buf);
        const line_opt = r.interface.takeDelimiter('\n') catch continue;
        var req = line_opt orelse continue;
        if (req.len > 0 and req[req.len - 1] == '\r') {
            req = req[0 .. req.len - 1];
        }

        var write_buf: [768]u8 = undefined;
        var w = conn.writer(ctx.io, &write_buf);

        if (std.mem.indexOf(u8, req, " /health") != null) {
            const body = "ok\n";
            w.interface.print("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", .{ body.len, body }) catch continue;
        } else if (std.mem.indexOf(u8, req, " /metrics") != null) {
            const count = ctx.total_jobs.load(.monotonic);
            const fails = ctx.failed_jobs.load(.monotonic);
            var body_buf: [384]u8 = undefined;
            const body = std.fmt.bufPrint(&body_buf,
                \\# HELP zig_jobs_processed_total Total number of jobs processed.
                \\# TYPE zig_jobs_processed_total counter
                \\zig_jobs_processed_total {d}
                \\# HELP zig_jobs_failed_total Total number of jobs failed / dead-lettered.
                \\# TYPE zig_jobs_failed_total counter
                \\zig_jobs_failed_total {d}
                \\
            , .{ count, fails }) catch continue;

            w.interface.print("HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", .{ body.len, body }) catch continue;
        } else {
            const body = "not found\n";
            w.interface.print("HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n{s}", .{ body.len, body }) catch continue;
        }
        w.interface.flush() catch continue;
    }
    logging.logJSON("info", null, "Metrics Server exited cleanly.");
}
