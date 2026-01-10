package com.example.raftkv;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Raft core (pragmatic):
 *  - Elections with wide, per-node–skewed timeouts to avoid split-vote
 *  - Immediate heartbeat on win + periodic heartbeats
 *  - Log replication for PUT; majority commit; apply to KV
 */
public class RaftNode {

    public interface KVStore {
        void put(byte[] key, byte[] value);
        byte[] get(byte[] key);
    }

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final Object mu = new Object();

    private final String id;
    private final Map<String,String> peers; // excludes self
    private final PeerClient rpc;
    private final KVStore kv;

    // role/state
    private volatile Role role = Role.FOLLOWER;
    private volatile int currentTerm = 0;
    private volatile String votedFor = null;
    private volatile String leaderId = null;
    private volatile int commitIndex = -1;
    private volatile int lastApplied = -1;

    // log
    private static final class LogEntry {
        final int term; final String key; final String val;
        LogEntry(int term, String key, String val) {
            this.term = term; this.key = key; this.val = val;
        }
    }
    private final List<LogEntry> log = new ArrayList<>();

    // replication bookkeeping (not deeply used here)
    private final Map<String,Integer> nextIndex = new HashMap<>();
    private final Map<String,Integer> matchIndex = new HashMap<>();

    // timers
    private final ScheduledExecutorService sched = Executors.newScheduledThreadPool(4);
    private ScheduledFuture<?> electionTask;
    private ScheduledFuture<?> heartbeatTask;
    private final Random rng = new Random();

    // election timeout window with per-node skew (ms)
    private final int minElectionMs;
    private final int maxElectionMs;

    public RaftNode(String id, Map<String,String> peers, KVStore kv, PeerClient rpc) {
        this.id = id;
        // copy & ensure we don't include ourselves as a peer
        LinkedHashMap<String,String> p = new LinkedHashMap<>(peers);
        p.remove(id);
        this.peers = Collections.unmodifiableMap(p);

        this.kv = kv;
        this.rpc = rpc;

        // deterministic skew from id to reduce simultaneous timeouts
        int skew = Math.floorMod(id.hashCode(), 600); // 0..599 ms
        this.minElectionMs = 2500 + skew;
        this.maxElectionMs = 5000 + skew;

        logInfo("init: peers=" + this.peers.keySet()
                + " timeout=[" + minElectionMs + "," + maxElectionMs + "]ms");

        resetElectionTimer();
    }

    // -------- status (used by /status) --------
    public String id() { return id; }
    public Role role() { return role; }
    public int term() { return currentTerm; }
    public String leaderId() { return leaderId; }
    public int commitIndex() { return commitIndex; }
    public int lastApplied() { return lastApplied; }

    // -------- KV helper --------
    byte[] kvGet(byte[] k) { return kv.get(k); }

    // -------- elections & heartbeats --------
    private void resetElectionTimer() {
        if (electionTask != null) electionTask.cancel(false);
        int span = Math.max(200, maxElectionMs - minElectionMs);
        int timeoutMs = minElectionMs + rng.nextInt(span + 1);
        electionTask = sched.schedule(this::onElectionTimeout, timeoutMs, TimeUnit.MILLISECONDS);
    }

    private void onElectionTimeout() {
        synchronized (mu) {
            if (role == Role.LEADER) return; // already leader
            role = Role.CANDIDATE;
            currentTerm++;
            votedFor = id;
            leaderId = null;
        }

        int lastIdx = lastLogIndex();
        int lastTerm = lastLogTerm();

        RequestVote.Request req = new RequestVote.Request();
        req.term = currentTerm;
        req.candidateId = id;
        req.lastLogIndex = lastIdx;
        req.lastLogTerm = lastTerm;

        AtomicInteger votes = new AtomicInteger(1); // self vote
        CountDownLatch latch = new CountDownLatch(peers.size());

        peers.forEach((pid, url) -> CompletableFuture.runAsync(() -> {
            RequestVote.Response r = rpc.requestVote(url, req);
            synchronized (mu) {
                if (r.term > currentTerm) {
                    stepDown(r.term);
                } else if (r.voteGranted && currentTerm == req.term) {
                    votes.incrementAndGet();
                }
            }
            latch.countDown();
        }));

        try { latch.await(2200, TimeUnit.MILLISECONDS); } catch (InterruptedException ignored) {}

        synchronized (mu) {
            if (role != Role.CANDIDATE) { resetElectionTimer(); return; }
            if (hasMajority(votes.get())) {
                role = Role.LEADER;
                leaderId = id;

                int ni = log.size();
                for (String pid : peers.keySet()) {
                    nextIndex.put(pid, ni);
                    matchIndex.put(pid, -1);
                }

                logInfo("became LEADER term=" + currentTerm + " logLast=" + lastIdx);

                // immediate heartbeat to assert leadership quickly
                AppendEntries.Request hb = baseAppendReq();
                hb.entries = List.of();
                broadcastAppend(hb, true);

                startHeartbeats();
            } else {
                // lost or tie → follower and retry later with new randomized timeout
                role = Role.FOLLOWER;
                votedFor = null;
                resetElectionTimer();
            }
        }
    }

    private void startHeartbeats() {
        if (heartbeatTask != null) heartbeatTask.cancel(false);
        heartbeatTask = sched.scheduleAtFixedRate(() -> {
            if (role != Role.LEADER) return;
            AppendEntries.Request hb = baseAppendReq();
            hb.entries = List.of(); // heartbeat
            try {
                broadcastAppend(hb, false);
            } catch (Exception ignored) {
            }
        }, 0, 200, TimeUnit.MILLISECONDS);
    }

    private void stepDown(int newTerm) {
        if (heartbeatTask != null) heartbeatTask.cancel(false);
        role = Role.FOLLOWER;
        currentTerm = newTerm;
        votedFor = null;
        leaderId = null;
        resetElectionTimer();
        logInfo("stepDown -> FOLLOWER term=" + currentTerm);
    }

    private boolean hasMajority(int yes) {
        int n = peers.size() + 1; // including self
        return yes > (n / 2);
    }

    // -------- RPC handlers --------
    public RequestVote.Response onRequestVote(RequestVote.Request req) {
        RequestVote.Response r = new RequestVote.Response();
        synchronized (mu) {
            if (req.term < currentTerm) {
                r.term = currentTerm; r.voteGranted = false; return r;
            }
            if (req.term > currentTerm) stepDown(req.term);

            boolean upToDate =
                    (req.lastLogTerm > lastLogTerm()) ||
                    (req.lastLogTerm == lastLogTerm() && req.lastLogIndex >= lastLogIndex());
            boolean canVote = (votedFor == null || votedFor.equals(req.candidateId));

            boolean ok = upToDate && canVote;
            if (ok) {
                votedFor = req.candidateId;
                resetElectionTimer();
            }
            r.term = currentTerm;
            r.voteGranted = ok;
            return r;
        }
    }

    public AppendEntries.Response onAppend(AppendEntries.Request req) {
        AppendEntries.Response r = new AppendEntries.Response();
        synchronized (mu) {
            if (req.term < currentTerm) {
                r.term = currentTerm; r.success = false; r.matchIndex = -1; return r;
            }
            if (req.term > currentTerm) stepDown(req.term);

            // accept leader
            role = Role.FOLLOWER;
            leaderId = req.leaderId;
            resetElectionTimer();

            // consistency check
            if (req.prevLogIndex >= 0) {
                if (req.prevLogIndex >= log.size()) {
                    r.term = currentTerm; r.success = false; r.matchIndex = log.size() - 1; return r;
                }
                if (log.get(req.prevLogIndex).term != req.prevLogTerm) {
                    r.term = currentTerm; r.success = false; r.matchIndex = req.prevLogIndex - 1; return r;
                }
            }

            // append new entries (truncate on conflict)
            int idx = (req.prevLogIndex < 0) ? 0 : req.prevLogIndex + 1;
            if (req.entries != null && !req.entries.isEmpty()) {
                while (log.size() > idx) log.remove(log.size() - 1);
                for (AppendEntries.Log kv : req.entries) {
                    log.add(new LogEntry(kv.term, kv.key, kv.val));
                }
            }

            // leader's commit
            if (req.leaderCommit > commitIndex) {
                commitIndex = Math.min(req.leaderCommit, log.size() - 1);
                applyToStateMachine();
            }

            r.term = currentTerm; r.success = true; r.matchIndex = log.size() - 1;
            return r;
        }
    }

    // -------- client PUT (robust replication with backoff) --------
    public boolean clientPut(String key, String val) {
        int entryIndex;
        int entryTerm;
        synchronized (mu) {
            if (role != Role.LEADER) return false;

            // append locally
            log.add(new LogEntry(currentTerm, key, val));
            entryIndex = log.size() - 1;
            entryTerm  = log.get(entryIndex).term;
        }

        AtomicInteger acks = new AtomicInteger(1); // self
        CountDownLatch latch = new CountDownLatch(peers.size());

        peers.forEach((pid, url) -> CompletableFuture.runAsync(() -> {
            // start from the leader's view of where follower should match:
            int prev = entryIndex - 1;
            int attempts = 0;

            while (attempts < 6) {
                AppendEntries.Request req = new AppendEntries.Request();
                synchronized (mu) {
                    if (role != Role.LEADER) break; // leadership changed
                    req.term = currentTerm;
                    req.leaderId = id;
                    req.leaderCommit = commitIndex;

                    req.prevLogIndex = prev;
                    req.prevLogTerm = (prev >= 0) ? log.get(prev).term : -1;

                    // one-entry replication (what we just appended)
                    req.entries = List.of(new AppendEntries.Log(entryTerm, key, val));
                }

                AppendEntries.Response resp = null;
                try {
                    resp = rpc.append(url, req);
                } catch (Exception ignored) {
                }

                if (resp == null) {
                    // network/parse issue: brief backoff and retry
                    sleep(200);
                    attempts++;
                    continue;
                }

                synchronized (mu) {
                    if (resp.term > currentTerm) { stepDown(resp.term); }
                }

                if (resp.success) {
                    acks.incrementAndGet();
                    break;
                } else {
                    // follower rejected: use hint (matchIndex) or back off by one
                    int hinted = resp.matchIndex;
                    prev = (hinted >= -1) ? hinted : (prev - 1);
                    attempts++;
                    sleep(100);
                }
            }
            latch.countDown();
        }));

        try { latch.await(5000, TimeUnit.MILLISECONDS); } catch (InterruptedException ignored) {}

        synchronized (mu) {
            if (hasMajority(acks.get()) && role == Role.LEADER) {
                commitIndex = entryIndex;
                applyToStateMachine();
                return true;
            } else {
                return false;
            }
        }
    }

    // -------- helpers --------
    private AppendEntries.Request baseAppendReq() {
        AppendEntries.Request r = new AppendEntries.Request();
        r.term = currentTerm;
        r.leaderId = id;
        r.prevLogIndex = lastLogIndex();
        r.prevLogTerm = lastLogTerm();
        r.leaderCommit = commitIndex;
        return r;
    }

    private void broadcastAppend(AppendEntries.Request base, boolean waitForMajority) {
        AtomicInteger acks = new AtomicInteger(1);
        CountDownLatch latch = new CountDownLatch(peers.size());

        peers.forEach((pid, url) -> CompletableFuture.runAsync(() -> {
            AppendEntries.Request req = new AppendEntries.Request();
            synchronized (mu) {
                req.term = base.term;
                req.leaderId = base.leaderId;
                req.prevLogIndex = base.prevLogIndex;
                req.prevLogTerm = base.prevLogTerm;
                req.entries = base.entries; // may be empty (heartbeat)
                req.leaderCommit = base.leaderCommit;
            }
            AppendEntries.Response resp = null;
            try { resp = rpc.append(url, req); } catch (Exception ignored) {}
            synchronized (mu) {
                if (resp != null && resp.term > currentTerm) { stepDown(resp.term); }
                if (resp != null && resp.success) acks.incrementAndGet();
            }
            latch.countDown();
        }));

        if (waitForMajority) {
            try { latch.await(1500, TimeUnit.MILLISECONDS); } catch (InterruptedException ignored) {}
        }
    }

    private void applyToStateMachine() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry e = log.get(lastApplied);
            kv.put(e.key.getBytes(), e.val.getBytes());
        }
    }

    private int lastLogIndex() { return log.size() - 1; }
    private int lastLogTerm()  { return log.isEmpty() ? -1 : log.get(log.size() - 1).term; }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }

    private static void logInfo(String s) { System.out.println("[raft] " + s); }
}