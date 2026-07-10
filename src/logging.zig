const std = @import("std");

/// Zero-allocation structured JSON logger (timestamp omitted; wire to Io.Timestamp if needed).
pub fn logJSON(level: []const u8, thread_id: ?usize, msg: []const u8) void {
    if (thread_id) |tid| {
        std.debug.print("{{\"level\":\"{s}\",\"thread_id\":{d},\"message\":\"{s}\"}}\n", .{ level, tid, msg });
    } else {
        std.debug.print("{{\"level\":\"{s}\",\"message\":\"{s}\"}}\n", .{ level, msg });
    }
}
