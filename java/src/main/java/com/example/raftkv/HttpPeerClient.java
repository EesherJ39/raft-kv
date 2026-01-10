package com.example.raftkv;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

public class HttpPeerClient implements PeerClient {

    private static String postJson(String url, String json) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setConnectTimeout(1000);
        conn.setReadTimeout(1500);
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        try (OutputStream os = conn.getOutputStream()) {
            os.write(json.getBytes(StandardCharsets.UTF_8));
        }
        int code = conn.getResponseCode();
        InputStream is = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();
        if (is == null) return null;
        try (is; ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
            is.transferTo(buf);
            return buf.toString(StandardCharsets.UTF_8);
        }
    }

    @Override
    public AppendEntries.Response append(String baseUrl, AppendEntries.Request req) {
        try {
            String body = RpcJson.appendReqToJson(req);
            String resp = postJson(baseUrl + "/append", body);
            return RpcJson.appendRespFromJson(resp);
        } catch (Exception e) {
            return null; // treat as no-ack
        }
    }

    @Override
    public RequestVote.Response requestVote(String baseUrl, RequestVote.Request req) {
        try {
            String body = RpcJson.reqVoteReqToJson(req);
            String resp = postJson(baseUrl + "/requestVote", body);
            return RpcJson.reqVoteRespFromJson(resp);
        } catch (Exception e) {
            return null; // treat as no-ack
        }
    }
}