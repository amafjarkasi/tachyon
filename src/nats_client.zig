const std = @import("std");
const Io = std.Io;
const net = Io.net;

pub const Config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 4222,
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,
    use_tls: bool = false,
    ca_path: ?[]const u8 = null,
};

pub const Connection = union(enum) {
    plain: struct {
        reader: net.Stream.Reader,
        writer: net.Stream.Writer,
    },
    tls: struct {
        tcp_reader: net.Stream.Reader,
        tcp_writer: net.Stream.Writer,
        client: std.crypto.tls.Client,
    },
};

pub const NatsClient = struct {
    io: Io,
    allocator: std.mem.Allocator,
    stream: net.Stream,
    reader_buf: []u8,
    writer_buf: []u8,
    tls_reader_buf: ?[]u8 = null,
    tls_writer_buf: ?[]u8 = null,
    connection: Connection,

    pub fn connect(io: Io, allocator: std.mem.Allocator, config: Config) !NatsClient {
        const peer = try net.IpAddress.parse(config.host, config.port);
        const stream = try peer.connect(io, .{ .mode = .stream });
        errdefer stream.close(io);

        // Allocate buffers for TCP stream
        const r_buf = try allocator.alloc(u8, 8192);
        errdefer allocator.free(r_buf);

        const w_buf = try allocator.alloc(u8, 8192);
        errdefer allocator.free(w_buf);

        var client = NatsClient{
            .io = io,
            .allocator = allocator,
            .stream = stream,
            .reader_buf = r_buf,
            .writer_buf = w_buf,
            .connection = undefined,
            .tls_reader_buf = null,
            .tls_writer_buf = null,
        };

        if (config.use_tls) {
            // Allocate extra buffers required by std.crypto.tls.Client (at least min_buffer_len = 16709)
            const tls_r_buf = try allocator.alloc(u8, 32768);
            errdefer allocator.free(tls_r_buf);
            client.tls_reader_buf = tls_r_buf;

            const tls_w_buf = try allocator.alloc(u8, 32768);
            errdefer allocator.free(tls_w_buf);
            client.tls_writer_buf = tls_w_buf;

            var entropy: [std.crypto.tls.Client.Options.entropy_len]u8 = undefined;
            try io.randomSecure(&entropy);

            const realtime_now = std.Io.Timestamp.now(io, .real);

            var tcp_reader = stream.reader(io, r_buf);
            var tcp_writer = stream.writer(io, w_buf);

            // Construct CA bundle
            var bundle = std.crypto.Certificate.Bundle.empty;
            errdefer bundle.deinit(allocator);

            if (config.ca_path) |ca_path| {
                if (std.fs.path.isAbsolute(ca_path)) {
                    try bundle.addCertsFromFilePathAbsolute(allocator, io, realtime_now, ca_path);
                } else {
                    try bundle.addCertsFromFilePath(allocator, io, realtime_now, std.Io.Dir.cwd(), ca_path);
                }
            } else {
                try bundle.rescan(allocator, io, realtime_now);
            }

            var rw_lock = std.Io.RwLock.init;

            const tls_options = std.crypto.tls.Client.Options{
                .host = .no_verification,
                .ca = .{
                    .bundle = .{
                        .gpa = allocator,
                        .io = io,
                        .lock = &rw_lock,
                        .bundle = &bundle,
                    },
                },
                .write_buffer = tls_w_buf,
                .read_buffer = tls_r_buf,
                .entropy = &entropy,
                .realtime_now = realtime_now,
            };

            const tls_client = try std.crypto.tls.Client.init(&tcp_reader.interface, &tcp_writer.interface, tls_options);
            bundle.deinit(allocator);

            client.connection = .{
                .tls = .{
                    .tcp_reader = tcp_reader,
                    .tcp_writer = tcp_writer,
                    .client = tls_client,
                },
            };
        } else {
            client.connection = .{
                .plain = .{
                    .reader = stream.reader(io, r_buf),
                    .writer = stream.writer(io, w_buf),
                },
            };
        }

        // 1. Read initial INFO message from server
        const line = try client.readLine() orelse return error.NoInfoFromServer;
        if (!std.mem.startsWith(u8, line, "INFO")) {
            return error.InvalidInfoFromServer;
        }

        // 2. Build and Send CONNECT message
        var connect_json = std.ArrayList(u8).empty;
        defer connect_json.deinit(allocator);

        try connect_json.appendSlice(allocator, "CONNECT {");
        try connect_json.appendSlice(allocator, "\"verbose\":false,\"pedantic\":false");
        if (config.use_tls) {
            try connect_json.appendSlice(allocator, ",\"ssl_required\":true");
        } else {
            try connect_json.appendSlice(allocator, ",\"ssl_required\":false");
        }
        if (config.username) |user| {
            try connect_json.appendSlice(allocator, ",\"user\":\"");
            try connect_json.appendSlice(allocator, user);
            try connect_json.appendSlice(allocator, "\"");
        }
        if (config.password) |pass| {
            try connect_json.appendSlice(allocator, ",\"pass\":\"");
            try connect_json.appendSlice(allocator, pass);
            try connect_json.appendSlice(allocator, "\"");
        }
        try connect_json.appendSlice(allocator, "}\r\n");

        const w = client.getWriter();
        try w.writeAll(connect_json.items);
        try w.flush();

        return client;
    }

    pub fn deinit(self: *NatsClient) void {
        self.stream.close(self.io);
        self.allocator.free(self.reader_buf);
        self.allocator.free(self.writer_buf);
        if (self.tls_reader_buf) |buf| self.allocator.free(buf);
        if (self.tls_writer_buf) |buf| self.allocator.free(buf);
    }

    fn getWriter(self: *NatsClient) *Io.Writer {
        return switch (self.connection) {
            .plain => |*p| &p.writer.interface,
            .tls => |*t| &t.client.writer,
        };
    }

    pub fn readLine(self: *NatsClient) !?[]const u8 {
        var line = switch (self.connection) {
            .plain => |*p| (try p.reader.interface.takeDelimiter('\n')) orelse return null,
            .tls => |*t| (try t.client.reader.takeDelimiter('\n')) orelse return null,
        };
        if (line.len > 0 and line[line.len - 1] == '\r') {
            line = line[0 .. line.len - 1];
        }
        return line;
    }

    pub fn publish(self: *NatsClient, subject: []const u8, reply_to: ?[]const u8, payload: []const u8) !void {
        try self.publishRaw(subject, reply_to, payload, true);
    }

    /// Write a PUB frame; when `do_flush` is false the caller must flush later (batch ACK).
    pub fn publishRaw(self: *NatsClient, subject: []const u8, reply_to: ?[]const u8, payload: []const u8, do_flush: bool) !void {
        const w = self.getWriter();
        if (reply_to) |reply| {
            try w.print("PUB {s} {s} {d}\r\n{s}\r\n", .{ subject, reply, payload.len, payload });
        } else {
            try w.print("PUB {s} {d}\r\n{s}\r\n", .{ subject, payload.len, payload });
        }
        if (do_flush) try w.flush();
    }

    /// Publish with NATS headers (HPUB). `headers` is a list of "Key: Value" lines (no trailing CRLF).
    pub fn publishWithHeaders(self: *NatsClient, subject: []const u8, reply_to: ?[]const u8, headers: []const []const u8, payload: []const u8) !void {
        var hdr_buf: [1024]u8 = undefined;
        var hdr_len: usize = 0;
        // NATS/1.0\r\n
        const version = "NATS/1.0\r\n";
        @memcpy(hdr_buf[hdr_len..][0..version.len], version);
        hdr_len += version.len;
        for (headers) |h| {
            if (hdr_len + h.len + 2 >= hdr_buf.len) return error.HeadersTooLarge;
            @memcpy(hdr_buf[hdr_len..][0..h.len], h);
            hdr_len += h.len;
            hdr_buf[hdr_len] = '\r';
            hdr_len += 1;
            hdr_buf[hdr_len] = '\n';
            hdr_len += 1;
        }
        // trailing blank line
        if (hdr_len + 2 >= hdr_buf.len) return error.HeadersTooLarge;
        hdr_buf[hdr_len] = '\r';
        hdr_len += 1;
        hdr_buf[hdr_len] = '\n';
        hdr_len += 1;

        const total = hdr_len + payload.len;
        const w = self.getWriter();
        if (reply_to) |reply| {
            try w.print("HPUB {s} {s} {d} {d}\r\n", .{ subject, reply, hdr_len, total });
        } else {
            try w.print("HPUB {s} {d} {d}\r\n", .{ subject, hdr_len, total });
        }
        try w.writeAll(hdr_buf[0..hdr_len]);
        try w.writeAll(payload);
        try w.writeAll("\r\n");
        try w.flush();
    }

    pub fn subscribe(self: *NatsClient, subject: []const u8, sid: []const u8) !void {
        const w = self.getWriter();
        try w.print("SUB {s} {s}\r\n", .{ subject, sid });
        try w.flush();
    }

    pub const Header = struct {
        key: []const u8,
        value: []const u8,
    };

    pub const Msg = struct {
        subject: []const u8,
        sid: []const u8,
        reply_to: ?[]const u8,
        payload: []const u8,
        headers: []Header,
        /// JetStream Nats-Delivery-Count (1 on first delivery). 0 if absent.
        delivery_count: u32,
        /// True when this is a JetStream status/empty response (404/408/etc).
        is_status: bool,
        status_code: u16,
        arena: std.heap.ArenaAllocator,

        pub fn deinit(self: *Msg) void {
            self.arena.deinit();
        }

        pub fn headerGet(self: *const Msg, key: []const u8) ?[]const u8 {
            for (self.headers) |h| {
                if (std.ascii.eqlIgnoreCase(h.key, key)) return h.value;
            }
            return null;
        }
    };

    fn readExact(self: *NatsClient, buf: []u8) !void {
        switch (self.connection) {
            .plain => |*p| try p.reader.interface.readSliceAll(buf),
            .tls => |*t| try t.client.reader.readSliceAll(buf),
        }
    }

    fn takeCrLf(self: *NatsClient) !void {
        var crlf: [2]u8 = undefined;
        try self.readExact(&crlf);
    }

    /// Parse NATS header block into Header slice on `alloc`. Also extracts status line.
    fn parseHeaders(alloc: std.mem.Allocator, raw: []const u8) !struct { headers: []Header, is_status: bool, status_code: u16, delivery_count: u32 } {
        var list: std.ArrayList(Header) = .empty;
        errdefer list.deinit(alloc);

        var is_status = false;
        var status_code: u16 = 0;
        var delivery_count: u32 = 0;

        var it = std.mem.splitSequence(u8, raw, "\r\n");
        if (it.next()) |first| {
            // "NATS/1.0" or "NATS/1.0 404 No Messages"
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
                const k = try alloc.dupe(u8, key);
                const v = try alloc.dupe(u8, value);
                try list.append(alloc, .{ .key = k, .value = v });
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

    pub fn readMsg(self: *NatsClient) !Msg {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();
        const alloc = arena.allocator();

        while (true) {
            const line = try self.readLine() orelse return error.ConnectionClosed;

            if (std.mem.startsWith(u8, line, "PING")) {
                const w = self.getWriter();
                try w.writeAll("PONG\r\n");
                try w.flush();
                continue;
            }

            if (std.mem.startsWith(u8, line, "-ERR")) {
                std.debug.print("NATS Error: {s}\n", .{line});
                return error.NatsServerError;
            }

            if (std.mem.startsWith(u8, line, "HMSG")) {
                // HMSG <subject> <sid> [reply-to] <hdr_len> <total_len>
                var it = std.mem.tokenizeAny(u8, line, " ");
                _ = it.next(); // HMSG
                const subject = try alloc.dupe(u8, it.next() orelse return error.InvalidMsgFormat);
                const sid = try alloc.dupe(u8, it.next() orelse return error.InvalidMsgFormat);

                const t3 = it.next() orelse return error.InvalidMsgFormat;
                const t4 = it.next() orelse return error.InvalidMsgFormat;
                var reply_to: ?[]const u8 = null;
                var hdr_len: usize = undefined;
                var total_len: usize = undefined;

                if (it.next()) |t5| {
                    reply_to = try alloc.dupe(u8, t3);
                    hdr_len = try std.fmt.parseInt(usize, t4, 10);
                    total_len = try std.fmt.parseInt(usize, t5, 10);
                } else {
                    hdr_len = try std.fmt.parseInt(usize, t3, 10);
                    total_len = try std.fmt.parseInt(usize, t4, 10);
                }
                if (hdr_len > total_len) return error.InvalidMsgFormat;

                const body = try alloc.alloc(u8, total_len);
                try self.readExact(body);
                try self.takeCrLf();

                const hdr_raw = body[0..hdr_len];
                const payload_buf = body[hdr_len..];
                const parsed = try parseHeaders(alloc, hdr_raw);

                return Msg{
                    .subject = subject,
                    .sid = sid,
                    .reply_to = reply_to,
                    .payload = payload_buf,
                    .headers = parsed.headers,
                    .delivery_count = if (parsed.delivery_count == 0) 1 else parsed.delivery_count,
                    .is_status = parsed.is_status or payload_buf.len == 0,
                    .status_code = parsed.status_code,
                    .arena = arena,
                };
            }

            if (std.mem.startsWith(u8, line, "MSG")) {
                var it = std.mem.tokenizeAny(u8, line, " ");
                _ = it.next(); // Skip "MSG"
                const subject = try alloc.dupe(u8, it.next() orelse return error.InvalidMsgFormat);
                const sid = try alloc.dupe(u8, it.next() orelse return error.InvalidMsgFormat);

                const token3 = it.next() orelse return error.InvalidMsgFormat;
                var reply_to: ?[]const u8 = null;
                var size_str: []const u8 = undefined;

                if (it.next()) |token4| {
                    reply_to = try alloc.dupe(u8, token3);
                    size_str = token4;
                } else {
                    size_str = token3;
                }

                const size = try std.fmt.parseInt(usize, size_str, 10);

                const payload_buf = try alloc.alloc(u8, size);
                try self.readExact(payload_buf);
                try self.takeCrLf();

                // Legacy status-without-headers: payload starts with NATS/1.0
                const is_status = std.mem.startsWith(u8, payload_buf, "NATS/1.0") or size == 0;

                return Msg{
                    .subject = subject,
                    .sid = sid,
                    .reply_to = reply_to,
                    .payload = payload_buf,
                    .headers = &[_]Header{},
                    .delivery_count = 1,
                    .is_status = is_status,
                    .status_code = 0,
                    .arena = arena,
                };
            }
        }
    }

    /// Create (or overwrite) a JetStream stream.
    /// `max_age_seconds` of 0 omits message TTL; otherwise sets stream `max_age` in nanoseconds.
    pub fn setupJetStream(self: *NatsClient, stream_name: []const u8, subjects: []const []const u8, max_age_seconds: u64) !void {
        const js_subject = try std.fmt.allocPrint(self.allocator, "$JS.API.STREAM.CREATE.{s}", .{stream_name});
        defer self.allocator.free(js_subject);

        var subjects_buf = std.ArrayList(u8).empty;
        defer subjects_buf.deinit(self.allocator);

        for (subjects, 0..) |subj, i| {
            if (i > 0) try subjects_buf.appendSlice(self.allocator, ",");
            try subjects_buf.appendSlice(self.allocator, "\"");
            try subjects_buf.appendSlice(self.allocator, subj);
            try subjects_buf.appendSlice(self.allocator, "\"");
        }

        const payload = if (max_age_seconds > 0) blk: {
            const max_age_ns = max_age_seconds * 1_000_000_000;
            break :blk try std.fmt.allocPrint(self.allocator,
                \\{{"name":"{s}","subjects":[{s}],"max_age":{d}}}
            , .{ stream_name, subjects_buf.items, max_age_ns });
        } else blk: {
            break :blk try std.fmt.allocPrint(self.allocator,
                \\{{"name":"{s}","subjects":[{s}]}}
            , .{ stream_name, subjects_buf.items });
        };
        defer self.allocator.free(payload);

        try self.publish(js_subject, null, payload);
    }

    /// Create a durable pull consumer with explicit ACK and max redelivery limit.
    pub fn setupConsumer(self: *NatsClient, stream_name: []const u8, consumer_name: []const u8, filter_subject: []const u8, max_deliver: u32) !void {
        const js_subject = try std.fmt.allocPrint(self.allocator, "$JS.API.CONSUMER.DURABLE.CREATE.{s}.{s}", .{ stream_name, consumer_name });
        defer self.allocator.free(js_subject);

        const config_json = try std.fmt.allocPrint(self.allocator,
            \\{{"stream_name":"{s}","config":{{"durable_name":"{s}","ack_policy":"explicit","filter_subject":"{s}","max_deliver":{d},"ack_wait":30000000000}}}}
        , .{ stream_name, consumer_name, filter_subject, max_deliver });
        defer self.allocator.free(config_json);

        try self.publish(js_subject, null, config_json);
    }

    pub fn requestNext(self: *NatsClient, stream_name: []const u8, consumer_name: []const u8, reply_to: []const u8, batch_size: usize) !void {
        const subject = try std.fmt.allocPrint(self.allocator, "$JS.API.CONSUMER.MSG.NEXT.{s}.{s}", .{ stream_name, consumer_name });
        defer self.allocator.free(subject);

        const payload = try std.fmt.allocPrint(self.allocator, "{{\"batch\":{d},\"expires\":5000000000}}", .{batch_size});
        defer self.allocator.free(payload);

        try self.publish(subject, reply_to, payload);
    }

    pub fn ack(self: *NatsClient, msg: *const Msg) !void {
        if (msg.reply_to) |reply| {
            try self.publishRaw(reply, null, "+ACK", true);
        }
    }

    /// Buffer a +ACK without flushing (pair with flush() for batch efficiency).
    pub fn ackBuffered(self: *NatsClient, msg: *const Msg) !void {
        if (msg.reply_to) |reply| {
            try self.publishRaw(reply, null, "+ACK", false);
        }
    }

    /// Terminal ACK: do not redeliver (used when max attempts exhausted → after DLQ).
    pub fn term(self: *NatsClient, msg: *const Msg) !void {
        if (msg.reply_to) |reply| {
            try self.publish(reply, null, "+TERM");
        }
    }

    /// In-progress ACK (extends ack_wait) — used by long-running jobs / progress.
    pub fn inProgress(self: *NatsClient, msg: *const Msg) !void {
        if (msg.reply_to) |reply| {
            try self.publish(reply, null, "+WPI");
        }
    }

    /// Negative-acknowledge a JetStream message so it can be redelivered.
    /// If `delay_ns` is non-null, JetStream waits that many nanoseconds before redelivery.
    pub fn nack(self: *NatsClient, msg: *const Msg, delay_ns: ?u64) !void {
        if (msg.reply_to) |reply| {
            if (delay_ns) |d| {
                var body_buf: [64]u8 = undefined;
                const body = try std.fmt.bufPrint(&body_buf, "-NAK {{\"delay\":{d}}}", .{d});
                try self.publish(reply, null, body);
            } else {
                try self.publish(reply, null, "-NAK");
            }
        }
    }

    /// Batch ACK helper: buffer +ACK for each message, then one flush.
    pub fn ackBatch(self: *NatsClient, messages: []const *const Msg) !void {
        for (messages) |msg| {
            try self.ackBuffered(msg);
        }
        try self.getWriter().flush();
    }

    /// Flush the write buffer only (no PING round-trip).
    pub fn flushWrites(self: *NatsClient) !void {
        try self.getWriter().flush();
    }

    pub fn flush(self: *NatsClient) !void {
        const w = self.getWriter();
        try w.writeAll("PING\r\n");
        try w.flush();

        while (true) {
            const line = try self.readLine() orelse return error.ConnectionClosed;
            if (std.mem.startsWith(u8, line, "PONG")) {
                break;
            }
        }
    }
};
