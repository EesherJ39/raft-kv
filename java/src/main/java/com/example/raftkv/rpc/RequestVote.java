package com.example.raftkv;

public class RequestVote {
    public static class Request {
        public int term;
        public String candidateId;
        public int lastLogIndex;
        public int lastLogTerm;
    }
    public static class Response {
        public int term;
        public boolean voteGranted;
    }
}