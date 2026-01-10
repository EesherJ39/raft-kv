package com.example.raftkv;

public class LogEntry {
    public int term;
    public String key;
    public String val;

    public LogEntry() {}
    public LogEntry(int term, String key, String val) {
        this.term = term;
        this.key = key;
        this.val = val;
    }
}