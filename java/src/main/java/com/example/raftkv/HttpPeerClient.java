package com.example.raftkv;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** HTTP/1.1 production transport for the binary internal Raft RPC protocol. */
public final class HttpPeerClient implements PeerClient {
    private static final int MAX_RESPONSE_BYTES = 8 * 1024 * 1024;
    private final HttpClient client;
    private final Duration timeout;

    public HttpPeerClient() {
        this(Duration.ofMillis(700));
    }

    public HttpPeerClient(Duration timeout) {
        this.timeout = timeout;
        this.client = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    @Override
    public RequestVote.Response requestVote(String peerId, String baseUrl, RequestVote.Request request) {
        try {
            return RpcCodec.decodeRequestVoteResponse(
                    post(baseUrl + "/raft/request-vote", RpcCodec.encodeRequestVote(request)));
        } catch (IOException | InterruptedException | RuntimeException error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public AppendEntries.Response appendEntries(String peerId, String baseUrl, AppendEntries.Request request) {
        try {
            return RpcCodec.decodeAppendEntriesResponse(
                    post(baseUrl + "/raft/append-entries", RpcCodec.encodeAppendEntries(request)));
        } catch (IOException | InterruptedException | RuntimeException error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public InstallSnapshot.Response installSnapshot(String peerId, String baseUrl, InstallSnapshot.Request request) {
        try {
            return RpcCodec.decodeInstallSnapshotResponse(
                    post(baseUrl + "/raft/install-snapshot", RpcCodec.encodeInstallSnapshot(request)));
        } catch (IOException | InterruptedException | RuntimeException error) {
            if (error instanceof InterruptedException) Thread.currentThread().interrupt();
            return null;
        }
    }

    private byte[] post(String url, byte[] body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(timeout)
                .header("Content-Type", "application/vnd.raftkv.rpc")
                .header("X-RaftKV-RPC-Version", "1")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() != 200) throw new IOException("RPC returned " + response.statusCode());
        if (response.body().length > MAX_RESPONSE_BYTES) throw new IOException("RPC response too large");
        return response.body();
    }
}
