package com.example.raftkv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Tiny HTTP layer:
 * - /status: text status for quick curl checks
 * - /get?key=K
 * - /put?key=K&val=V  (307 redirect if this node isn’t leader)
 * - /requestVote  and /raft/requestVote  (both accepted)
 * - /append       and /raft/append       (both accepted)
 */
public class KeyValueService {

    private final RaftNode raft;
    private final int port;

    public KeyValueService(RaftNode raft, int port) {
        this.raft = raft;
        this.port = port;
    }

    public void serve() throws IOException {
        HttpServer http = HttpServer.create(new InetSocketAddress(port), 0);

        http.createContext("/status", this::handleStatus);
        http.createContext("/get", this::handleGet);
        http.createContext("/put", this::handlePut);

        // Accept both legacy and namespaced RPC paths
        http.createContext("/requestVote", this::handleRequestVote);
        http.createContext("/raft/requestVote", this::handleRequestVote);

        http.createContext("/append", this::handleAppend);
        http.createContext("/raft/append", this::handleAppend);

        http.setExecutor(null);
        http.start();

        System.out.println("RaftKV listening on http://localhost:" + port + " id=" + raft.id());
    }

    // ------------------- Handlers -------------------

    private void handleStatus(HttpExchange ex) throws IOException {
        String body = "role=" + raft.role()
                + ", term=" + raft.term()
                + ", leaderId=" + raft.leaderId()
                + ", commitIndex=" + raft.commitIndex()
                + ", lastApplied=" + raft.lastApplied();
        write(ex, 200, body);
    }

    private void handleGet(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex.getRequestURI());
        String key = q.get("key");
        if (key == null) {
            write(ex, 400, "missing key");
            return;
        }
        byte[] v = raft.kvGet(key.getBytes(StandardCharsets.UTF_8));
        if (v == null) {
            write(ex, 200, "NOT_FOUND");
        } else {
            write(ex, 200, new String(v, StandardCharsets.UTF_8));
        }
    }

    private void handlePut(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex.getRequestURI());
        String key = q.get("key");
        String val = q.get("val");
        if (key == null || val == null) {
            write(ex, 400, "missing key/val");
            return;
        }

        // Redirect if we’re not the leader
        if (raft.role() != RaftNode.Role.LEADER) {
            ex.getResponseHeaders().add("X-role", "FOLLOWER");
            write(ex, 307, "REDIRECT_TO_LEADER");
            return;
        }

        boolean ok = raft.clientPut(key, val);
        write(ex, ok ? 200 : 500, ok ? "OK" : "REPL_FAIL");
    }

    private void handleRequestVote(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            write(ex, 405, "use POST");
            return;
        }
        RequestVote.Request req;
        try {
            req = Json.M.readValue(ex.getRequestBody(), RequestVote.Request.class);
        } catch (Exception e) {
            write(ex, 400, "bad json");
            return;
        }
        RequestVote.Response resp = raft.onRequestVote(req);
        writeJson(ex, 200, resp);
    }

    private void handleAppend(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            write(ex, 405, "use POST");
            return;
        }
        AppendEntries.Request req;
        try {
            req = Json.M.readValue(ex.getRequestBody(), AppendEntries.Request.class);
        } catch (Exception e) {
            write(ex, 400, "bad json");
            return;
        }
        AppendEntries.Response resp = raft.onAppend(req);
        writeJson(ex, 200, resp);
    }

    // ------------------- Helpers -------------------

    private Map<String, String> query(URI u) {
        Map<String, String> m = new HashMap<>();
        String qs = u.getRawQuery();
        if (qs == null || qs.isEmpty()) return m;
        for (String p : qs.split("&")) {
            int i = p.indexOf('=');
            if (i < 0) continue;
            String k = decode(p.substring(0, i));
            String v = decode(p.substring(i + 1));
            m.put(k, v);
        }
        return m;
    }

    private String decode(String s) {
        return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private void write(HttpExchange ex, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private void writeJson(HttpExchange ex, int code, Object obj) throws IOException {
        byte[] bytes = Json.M.writeValueAsBytes(obj);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }
}