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
        const w = self.getWriter();
        if (reply_to) |reply| {
            try w.print("PUB {s} {s} {d}\r\n{s}\r\n", .{ subject, reply, payload.len, payload });
        } else {
            try w.print("PUB {s} {d}\r\n{s}\r\n", .{ subject, payload.len, payload });
        }
        try w.flush();
    }

    pub fn subscribe(self: *NatsClient, subject: []const u8, sid: []const u8) !void {
        const w = self.getWriter();
        try w.print("SUB {s} {s}\r\n", .{ subject, sid });
        try w.flush();
    }

    pub const Msg = struct {
        subject: []const u8,
        sid: []const u8,
        reply_to: ?[]const u8,
        payload: []const u8,
        arena: std.heap.ArenaAllocator,

        pub fn deinit(self: *Msg) void {
            self.arena.deinit();
        }
    };

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
                switch (self.connection) {
                    .plain => |*p| {
                        try p.reader.interface.readSliceAll(payload_buf);
                        _ = try p.reader.interface.takeByte();
                        _ = try p.reader.interface.takeByte();
                    },
                    .tls => |*t| {
                        try t.client.reader.readSliceAll(payload_buf);
                        _ = try t.client.reader.takeByte();
                        _ = try t.client.reader.takeByte();
                    },
                }

                return Msg{
                    .subject = subject,
                    .sid = sid,
                    .reply_to = reply_to,
                    .payload = payload_buf,
                    .arena = arena,
                };
            }
        }
    }

    pub fn setupJetStream(self: *NatsClient, stream_name: []const u8, subjects: []const []const u8) !void {
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

        const payload = try std.fmt.allocPrint(self.allocator,
            \\{{"name":"{s}","subjects":[{s}]}}
        , .{ stream_name, subjects_buf.items });
        defer self.allocator.free(payload);

        try self.publish(js_subject, null, payload);
    }

    pub fn setupConsumer(self: *NatsClient, stream_name: []const u8, consumer_name: []const u8, filter_subject: []const u8) !void {
        const js_subject = try std.fmt.allocPrint(self.allocator, "$JS.API.CONSUMER.DURABLE.CREATE.{s}.{s}", .{ stream_name, consumer_name });
        defer self.allocator.free(js_subject);

        const config_json = try std.fmt.allocPrint(self.allocator,
            \\{{"stream_name":"{s}","config":{{"durable_name":"{s}","ack_policy":"explicit","filter_subject":"{s}"}}}}
        , .{ stream_name, consumer_name, filter_subject });
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
            try self.publish(reply, null, "+ACK");
        }
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
