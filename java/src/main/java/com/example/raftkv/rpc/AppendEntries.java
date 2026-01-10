package com.example.raftkv;

import java.util.List;

/**
 * POJOs for Raft AppendEntries RPC.
 * Public fields so Jackson can (de)serialize without boilerplate.
 */
public final class AppendEntries {

    /** Leader -> follower request. */
    public static final class Request {
        public int term;              // leader’s term
        public String leaderId;       // leader id
        public int prevLogIndex = -1; // index of log entry immediately preceding new ones
        public int prevLogTerm = -1;  // term of prevLogIndex entry
        public List<Log> entries;     // empty for heartbeat
        public int leaderCommit = -1; // leader’s commitIndex

        public Request() {}
    }

    /** Follower -> leader response. */
    public static final class Response {
        public int term;          // currentTerm for leader to update itself
        public boolean success;   // true if follower contained entry matching prevLogIndex/prevLogTerm
        public int matchIndex;    // last index that matches after this RPC (or hint)

        public Response() {}
    }

    /** A single replicated log entry (PUT key/value). */
    public static final class Log {
        public int term;
        public String key;
        public String val;

        public Log() {} // for Jackson
        public Log(int term, String key, String val) {
            this.term = term;
            this.key = key;
            this.val = val;
        }
    }
}