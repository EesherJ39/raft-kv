package com.example.raftkv;

/** A bounded diagnostic event exposed by the HTTP trace endpoint. */
public record RaftTraceEvent(
        long sequence,
        long epochMillis,
        String nodeId,
        int term,
        String role,
        String event,
        String detail) {}
