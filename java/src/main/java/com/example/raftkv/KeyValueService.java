package com.example.raftkv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Public KV API plus bounded internal Raft RPC endpoints. */
public final class KeyValueService implements AutoCloseable {
    private static final int MAX_CLIENT_VALUE_BYTES = 1 * 1024 * 1024;
    private static final int MAX_RPC_BYTES = 64 * 1024 * 1024;
    private static final String KV_PREFIX = "/v1/kv/";
    private final RaftNode raft;
    private final HttpServer server;
    private final ExecutorService executor;

    public KeyValueService(RaftNode raft, int port) throws IOException {
        this.raft = raft;
        server = HttpServer.create(new InetSocketAddress(port), 64);
        executor = Executors.newFixedThreadPool(
                Math.max(4, Runtime.getRuntime().availableProcessors()),
                runnable -> {
                    Thread thread = new Thread(runnable, "raft-http");
                    thread.setDaemon(true);
                    return thread;
                });
        server.setExecutor(executor);
        server.createContext("/", this::handleRoot);
        server.createContext("/healthz", exchange -> writeText(exchange, 200, "ok"));
        server.createContext("/v1/status", this::handleStatus);
        server.createContext("/v1/trace", this::handleTrace);
        server.createContext("/v1/kv", this::handleKeyValue);
        server.createContext("/raft/request-vote", this::handleRequestVote);
        server.createContext("/raft/append-entries", this::handleAppendEntries);
        server.createContext("/raft/install-snapshot", this::handleInstallSnapshot);
    }

    public void start() {
        server.start();
    }

    private void handleRoot(HttpExchange exchange) throws IOException {
        if (!"/".equals(exchange.getRequestURI().getPath())) {
            writeJson(exchange, 404, "{\"error\":\"not found\"}");
            return;
        }
        if (!requireMethod(exchange, "GET")) return;
        byte[] body = TraceViewer.page().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        write(exchange, 200, body);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "GET")) return;
        RaftNode.Status status = raft.status();
        String json = "{"
                + field("id", status.id()) + ","
                + field("role", status.role().name()) + ","
                + number("term", status.term()) + ","
                + nullableField("leaderId", status.leaderId()) + ","
                + number("commitIndex", status.commitIndex()) + ","
                + number("lastApplied", status.lastApplied()) + ","
                + number("lastLogIndex", status.lastLogIndex()) + ","
                + number("snapshotIndex", status.snapshotIndex()) + ","
                + number("retainedLogEntries", status.retainedLogEntries())
                + "}";
        writeJson(exchange, 200, json);
    }

    private void handleTrace(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "GET")) return;
        List<RaftTraceEvent> events = raft.traceSnapshot();
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < events.size(); i++) {
            if (i > 0) json.append(',');
            RaftTraceEvent event = events.get(i);
            json.append('{')
                    .append(number("sequence", event.sequence())).append(',')
                    .append(number("epochMillis", event.epochMillis())).append(',')
                    .append(field("nodeId", event.nodeId())).append(',')
                    .append(number("term", event.term())).append(',')
                    .append(field("role", event.role())).append(',')
                    .append(field("event", event.event())).append(',')
                    .append(field("detail", event.detail())).append('}');
        }
        json.append(']');
        writeJson(exchange, 200, json.toString());
    }

    private void handleKeyValue(HttpExchange exchange) throws IOException {
        String rawPath = exchange.getRequestURI().getRawPath();
        if (!rawPath.startsWith(KV_PREFIX) || rawPath.length() == KV_PREFIX.length()) {
            writeJson(exchange, 400, "{\"error\":\"use /v1/kv/{key}\"}");
            return;
        }
        String key = URLDecoder.decode(rawPath.substring(KV_PREFIX.length()), StandardCharsets.UTF_8);
        if (key.isBlank()) {
            writeJson(exchange, 400, "{\"error\":\"key is required\"}");
            return;
        }

        if ("PUT".equalsIgnoreCase(exchange.getRequestMethod())) {
            byte[] body;
            try {
                body = readBounded(exchange, MAX_CLIENT_VALUE_BYTES);
            } catch (PayloadTooLargeException error) {
                writeJson(exchange, 413, "{\"error\":\"value exceeds 1 MiB\"}");
                return;
            }
            String value = new String(body, StandardCharsets.UTF_8);
            RaftNode.WriteResult result;
            try {
                result = raft.put(key, value);
            } catch (IllegalArgumentException error) {
                writeJson(exchange, 413, "{\"error\":" + quote(error.getMessage()) + "}");
                return;
            }
            switch (result.code()) {
                case OK -> writeJson(exchange, 201, "{\"ok\":true,\"index\":" + result.logIndex() + "}");
                case NOT_LEADER -> redirectOrUnavailable(exchange);
                case NO_QUORUM -> writeJson(exchange, 503, "{\"error\":\"quorum unavailable\"}");
                default -> writeJson(exchange, 500, "{\"error\":\"unexpected write result\"}");
            }
            return;
        }

        if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            RaftNode.ReadResult result = raft.getLinearizable(key);
            switch (result.code()) {
                case OK -> writeJson(exchange, 200, "{\"key\":" + quote(key) + ",\"value\":" + quote(result.value()) + "}");
                case NOT_FOUND -> writeJson(exchange, 404, "{\"error\":\"not found\"}");
                case NOT_LEADER -> redirectOrUnavailable(exchange);
                case NO_QUORUM -> writeJson(exchange, 503, "{\"error\":\"linearizable read quorum unavailable\"}");
            }
            return;
        }

        exchange.getResponseHeaders().set("Allow", "GET, PUT");
        writeJson(exchange, 405, "{\"error\":\"method not allowed\"}");
    }

    private void redirectOrUnavailable(HttpExchange exchange) throws IOException {
        String leaderUrl = raft.leaderUrl().orElse(null);
        if (leaderUrl == null) {
            writeJson(exchange, 503, "{\"error\":\"leader unknown\"}");
            return;
        }
        String location = leaderUrl + exchange.getRequestURI().getRawPath();
        exchange.getResponseHeaders().set("Location", location);
        exchange.getResponseHeaders().set("X-Raft-Leader", leaderUrl);
        writeJson(exchange, 307, "{\"error\":\"not leader\",\"leader\":" + quote(leaderUrl) + "}");
    }

    private void handleRequestVote(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "POST")) return;
        try {
            RequestVote.Request request = RpcCodec.decodeRequestVote(readBounded(exchange, MAX_RPC_BYTES));
            writeRpc(exchange, RpcCodec.encodeRequestVoteResponse(raft.onRequestVote(request)));
        } catch (IOException | IllegalArgumentException error) {
            writeJson(exchange, 400, "{\"error\":\"invalid RequestVote payload\"}");
        }
    }

    private void handleAppendEntries(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "POST")) return;
        try {
            AppendEntries.Request request = RpcCodec.decodeAppendEntries(readBounded(exchange, MAX_RPC_BYTES));
            writeRpc(exchange, RpcCodec.encodeAppendEntriesResponse(raft.onAppendEntries(request)));
        } catch (IOException | IllegalArgumentException error) {
            writeJson(exchange, 400, "{\"error\":\"invalid AppendEntries payload\"}");
        }
    }

    private void handleInstallSnapshot(HttpExchange exchange) throws IOException {
        if (!requireMethod(exchange, "POST")) return;
        try {
            InstallSnapshot.Request request = RpcCodec.decodeInstallSnapshot(readBounded(exchange, MAX_RPC_BYTES));
            writeRpc(exchange, RpcCodec.encodeInstallSnapshotResponse(raft.onInstallSnapshot(request)));
        } catch (IOException | IllegalArgumentException error) {
            writeJson(exchange, 400, "{\"error\":\"invalid InstallSnapshot payload\"}");
        }
    }

    private static boolean requireMethod(HttpExchange exchange, String expected) throws IOException {
        if (expected.equalsIgnoreCase(exchange.getRequestMethod())) return true;
        exchange.getResponseHeaders().set("Allow", expected);
        writeJson(exchange, 405, "{\"error\":\"method not allowed\"}");
        return false;
    }

    private static byte[] readBounded(HttpExchange exchange, int limit) throws IOException {
        byte[] body = exchange.getRequestBody().readNBytes(limit + 1);
        if (body.length > limit) throw new PayloadTooLargeException();
        return body;
    }

    private static final class PayloadTooLargeException extends IOException {
        private static final long serialVersionUID = 1L;
    }

    private static void writeRpc(HttpExchange exchange, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/vnd.raftkv.rpc");
        write(exchange, 200, body);
    }

    private static void writeJson(HttpExchange exchange, int status, String json) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        write(exchange, status, json.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeText(HttpExchange exchange, int status, String text) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        write(exchange, status, text.getBytes(StandardCharsets.UTF_8));
    }

    private static void write(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        } finally {
            exchange.close();
        }
    }

    private static String field(String key, String value) {
        return quote(key) + ":" + quote(value);
    }

    private static String nullableField(String key, String value) {
        return quote(key) + ":" + (value == null ? "null" : quote(value));
    }

    private static String number(String key, long value) {
        return quote(key) + ":" + value;
    }

    static String quote(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) escaped.append(String.format("\\u%04x", (int) c));
                    else escaped.append(c);
                }
            }
        }
        return escaped.append('"').toString();
    }

    @Override
    public void close() {
        server.stop(1);
        executor.shutdownNow();
    }
}
