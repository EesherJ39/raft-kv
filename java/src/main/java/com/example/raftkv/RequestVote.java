package com.example.raftkv;

/** RequestVote RPC messages from the Raft paper. */
public final class RequestVote {
    private RequestVote() {}

    public record Request(int term, String candidateId, long lastLogIndex, int lastLogTerm) {}
    public record Response(int term, boolean voteGranted) {}
}
