package com.example.raftkv;

/** Transport boundary: HTTP in production and a fault-injected network in tests. */
public interface PeerClient {
    RequestVote.Response requestVote(String peerId, String baseUrl, RequestVote.Request request);
    AppendEntries.Response appendEntries(String peerId, String baseUrl, AppendEntries.Request request);
    InstallSnapshot.Response installSnapshot(String peerId, String baseUrl, InstallSnapshot.Request request);
}
