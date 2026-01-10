package com.example.raftkv;

public interface PeerClient {
    AppendEntries.Response append(String baseUrl, AppendEntries.Request req);
    RequestVote.Response  requestVote(String baseUrl, RequestVote.Request req);
}