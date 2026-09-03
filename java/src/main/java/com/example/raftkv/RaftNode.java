package com.example.raftkv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A compact Raft implementation with persisted hard state, conflict-directed log
 * repair, current-term commit rules, quorum-checked reads, and snapshot catch-up.
 *
 * All protocol state is guarded by {@code mutex}. Network calls are made outside
 * the mutex so one unavailable peer cannot block inbound Raft RPC handling.
 */
public final class RaftNode implements AutoCloseable {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }
    public enum ResultCode { OK, NOT_FOUND, NOT_LEADER, NO_QUORUM }

    public record WriteResult(ResultCode code, String leaderId, long logIndex) {}
    public record ReadResult(ResultCode code, String value, String leaderId) {}
    public record Status(
            String id,
            Role role,
            int term,
            String leaderId,
            long commitIndex,
            long lastApplied,
            long lastLogIndex,
            long snapshotIndex,
            int retainedLogEntries) {}
    public record ReplicationResult(int acknowledgements, int clusterSize, boolean leader) {
        public boolean hasQuorum() {
            return leader && acknowledgements > clusterSize / 2;
        }
    }

    private static final int MAX_KEY_CHARS = 4_096;
    private static final int MAX_VALUE_CHARS = 1_048_576;

    private final Object mutex = new Object();
    private final String id;
    private final Map<String, String> clusterUrls;
    private final Map<String, String> peers;
    private final PeerClient transport;
    private final KeyValueStateMachine stateMachine;
    private final RaftStorage storage;
    private final RaftConfig config;
    private final Clock clock;
    private final SplittableRandom random;
    private final ArrayDeque<RaftTraceEvent> trace = new ArrayDeque<>();

    private Role role = Role.FOLLOWER;
    private int currentTerm;
    private String votedFor;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private RaftStorage.Snapshot snapshot;
    private final ArrayList<LogEntry> log = new ArrayList<>();
    private final Map<String, Long> nextIndex = new LinkedHashMap<>();
    private final Map<String, Long> matchIndex = new LinkedHashMap<>();

    private long electionDeadlineMs;
    private long nextHeartbeatMs;
    private long traceSequence;
    private ScheduledExecutorService scheduler;
    private boolean closed;

    public RaftNode(
            String id,
            Map<String, String> clusterUrls,
            PeerClient transport,
            KeyValueStateMachine stateMachine,
            RaftStorage storage,
            RaftConfig config) {
        this(id, clusterUrls, transport, stateMachine, storage, config, Clock.systemUTC());
    }

    RaftNode(
            String id,
            Map<String, String> clusterUrls,
            PeerClient transport,
            KeyValueStateMachine stateMachine,
            RaftStorage storage,
            RaftConfig config,
            Clock clock) {
        this.id = requireNodeId(id);
        this.clusterUrls = Map.copyOf(clusterUrls);
        if (!this.clusterUrls.containsKey(id)) {
            throw new IllegalArgumentException("cluster URLs must include this node: " + id);
        }
        LinkedHashMap<String, String> peerCopy = new LinkedHashMap<>(clusterUrls);
        peerCopy.remove(id);
        this.peers = Map.copyOf(peerCopy);
        this.transport = Objects.requireNonNull(transport, "transport");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
        this.storage = Objects.requireNonNull(storage, "storage");
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = new SplittableRandom(0x5EEDL ^ id.hashCode());

        RaftStorage.StoredState stored = loadState();
        currentTerm = stored.currentTerm();
        votedFor = stored.votedFor();
        commitIndex = stored.commitIndex();
        snapshot = stored.snapshot();
        log.addAll(stored.log());
        restoreCommittedStateLocked();
        resetElectionDeadlineLocked(now());
        emitLocked("recovered", "commit=" + commitIndex + ", snapshot=" + snapshot.lastIncludedIndex());
    }

    public void start() {
        synchronized (mutex) {
            ensureOpenLocked();
            if (scheduler != null) return;
            ThreadFactory factory = runnable -> {
                Thread thread = new Thread(runnable, "raft-timer-" + id);
                thread.setDaemon(true);
                return thread;
            };
            scheduler = Executors.newSingleThreadScheduledExecutor(factory);
            scheduler.scheduleWithFixedDelay(this::safeTick, 10, 10, TimeUnit.MILLISECONDS);
        }
    }

    private void safeTick() {
        try {
            tick(now());
        } catch (RuntimeException e) {
            synchronized (mutex) {
                emitLocked("timer-error", e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
    }

    public void tick(long nowMs) {
        boolean shouldCampaign;
        boolean shouldHeartbeat;
        synchronized (mutex) {
            if (closed) return;
            shouldCampaign = role != Role.LEADER && nowMs >= electionDeadlineMs;
            shouldHeartbeat = role == Role.LEADER && nowMs >= nextHeartbeatMs;
            if (shouldHeartbeat) nextHeartbeatMs = nowMs + config.heartbeatMs();
        }
        if (shouldCampaign) campaign();
        else if (shouldHeartbeat) replicateNow();
    }

    /** Starts an election immediately. Public to make deterministic testing explicit. */
    public boolean campaign() {
        final RequestVote.Request request;
        final int electionTerm;
        synchronized (mutex) {
            ensureOpenLocked();
            role = Role.CANDIDATE;
            leaderId = null;
            currentTerm++;
            electionTerm = currentTerm;
            votedFor = id;
            resetElectionDeadlineLocked(now());
            persistLocked();
            request = new RequestVote.Request(currentTerm, id, lastLogIndexLocked(), lastLogTermLocked());
            emitLocked("campaign", "lastLog=" + request.lastLogIndex() + "/" + request.lastLogTerm());
        }

        int votes = 1;
        for (Map.Entry<String, String> peer : peers.entrySet()) {
            RequestVote.Response response = transport.requestVote(peer.getKey(), peer.getValue(), request);
            if (response == null) continue;
            synchronized (mutex) {
                if (response.term() > currentTerm) {
                    becomeFollowerLocked(response.term(), null, "higher-term vote response");
                } else if (role == Role.CANDIDATE
                        && currentTerm == electionTerm
                        && response.voteGranted()) {
                    votes++;
                }
            }
        }

        boolean won = false;
        synchronized (mutex) {
            if (role == Role.CANDIDATE && currentTerm == electionTerm && hasMajority(votes)) {
                becomeLeaderLocked();
                won = true;
            } else if (role == Role.CANDIDATE && currentTerm == electionTerm) {
                role = Role.FOLLOWER;
                resetElectionDeadlineLocked(now());
                emitLocked("election-lost", "votes=" + votes);
            }
        }
        if (won) replicateNow();
        return won;
    }

    public RequestVote.Response onRequestVote(RequestVote.Request request) {
        Objects.requireNonNull(request, "request");
        synchronized (mutex) {
            ensureOpenLocked();
            if (request.term() < currentTerm) return new RequestVote.Response(currentTerm, false);
            if (request.term() > currentTerm) {
                becomeFollowerLocked(request.term(), null, "higher-term vote request");
            }

            boolean candidateUpToDate = request.lastLogTerm() > lastLogTermLocked()
                    || (request.lastLogTerm() == lastLogTermLocked()
                    && request.lastLogIndex() >= lastLogIndexLocked());
            boolean canVote = votedFor == null || votedFor.equals(request.candidateId());
            boolean granted = canVote && candidateUpToDate;
            if (granted) {
                votedFor = request.candidateId();
                resetElectionDeadlineLocked(now());
                persistLocked();
                emitLocked("vote-granted", "candidate=" + request.candidateId());
            }
            return new RequestVote.Response(currentTerm, granted);
        }
    }

    public AppendEntries.Response onAppendEntries(AppendEntries.Request request) {
        Objects.requireNonNull(request, "request");
        synchronized (mutex) {
            ensureOpenLocked();
            if (request.term() < currentTerm) {
                return appendFailureLocked(lastLogIndexLocked() + 1, -1);
            }
            if (request.term() > currentTerm) {
                becomeFollowerLocked(request.term(), request.leaderId(), "higher-term append");
            } else if (role != Role.FOLLOWER || !Objects.equals(leaderId, request.leaderId())) {
                role = Role.FOLLOWER;
                leaderId = request.leaderId();
                emitLocked("leader-observed", "leader=" + leaderId);
            }
            leaderId = request.leaderId();
            resetElectionDeadlineLocked(now());

            if (request.prevLogIndex() < snapshot.lastIncludedIndex()) {
                return new AppendEntries.Response(
                        currentTerm, true, snapshot.lastIncludedIndex(), snapshot.lastIncludedIndex() + 1, -1);
            }
            if (request.prevLogIndex() == snapshot.lastIncludedIndex()
                    && request.prevLogIndex() >= 0
                    && request.prevLogTerm() != snapshot.lastIncludedTerm()) {
                return appendFailureLocked(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
            }
            if (request.prevLogIndex() > lastLogIndexLocked()) {
                return appendFailureLocked(lastLogIndexLocked() + 1, -1);
            }
            if (request.prevLogIndex() > snapshot.lastIncludedIndex()) {
                int localTerm = termAtLocked(request.prevLogIndex());
                if (localTerm != request.prevLogTerm()) {
                    return appendFailureLocked(firstIndexOfTermLocked(localTerm), localTerm);
                }
            }

            long expectedIndex = request.prevLogIndex() + 1;
            for (LogEntry incoming : request.entries()) {
                if (incoming.index() != expectedIndex || incoming.term() > request.term()) {
                    emitLocked("invalid-append", "expected index " + expectedIndex);
                    return appendFailureLocked(lastLogIndexLocked() + 1, -1);
                }
                expectedIndex++;
            }

            boolean changed = false;
            for (LogEntry incoming : request.entries()) {
                if (incoming.index() <= snapshot.lastIncludedIndex()) continue;
                if (incoming.index() <= lastLogIndexLocked()) {
                    LogEntry local = entryAtLocked(incoming.index());
                    if (!local.equals(incoming)) {
                        if (incoming.index() <= commitIndex) {
                            emitLocked("safety-reject", "attempted overwrite of committed index " + incoming.index());
                            return appendFailureLocked(incoming.index(), local.term());
                        }
                        truncateFromLocked(incoming.index());
                        log.add(incoming);
                        changed = true;
                    }
                } else if (incoming.index() == lastLogIndexLocked() + 1) {
                    log.add(incoming);
                    changed = true;
                } else {
                    return appendFailureLocked(lastLogIndexLocked() + 1, -1);
                }
            }

            long newCommit = Math.min(request.leaderCommit(), lastLogIndexLocked());
            if (newCommit > commitIndex) {
                commitIndex = newCommit;
                applyCommittedLocked();
                changed = true;
            }
            if (changed) persistLocked();
            long matchedThrough = request.entries().isEmpty()
                    ? request.prevLogIndex()
                    : request.entries().get(request.entries().size() - 1).index();
            return new AppendEntries.Response(
                    currentTerm, true, matchedThrough, lastLogIndexLocked() + 1, -1);
        }
    }

    public InstallSnapshot.Response onInstallSnapshot(InstallSnapshot.Request request) {
        Objects.requireNonNull(request, "request");
        synchronized (mutex) {
            ensureOpenLocked();
            if (request.term() < currentTerm) {
                return new InstallSnapshot.Response(currentTerm, false, snapshot.lastIncludedIndex());
            }
            if (request.term() > currentTerm) {
                becomeFollowerLocked(request.term(), request.leaderId(), "higher-term snapshot");
            } else {
                role = Role.FOLLOWER;
                leaderId = request.leaderId();
            }
            resetElectionDeadlineLocked(now());
            if (request.lastIncludedIndex() <= snapshot.lastIncludedIndex()) {
                return new InstallSnapshot.Response(currentTerm, true, snapshot.lastIncludedIndex());
            }

            List<LogEntry> retained = List.of();
            if (request.lastIncludedIndex() <= lastLogIndexLocked()
                    && termAtLocked(request.lastIncludedIndex()) == request.lastIncludedTerm()) {
                retained = entriesAfterLocked(request.lastIncludedIndex());
            }

            snapshot = new RaftStorage.Snapshot(
                    request.lastIncludedIndex(),
                    request.lastIncludedTerm(),
                    request.values());
            log.clear();
            log.addAll(retained);
            commitIndex = Math.max(snapshot.lastIncludedIndex(),
                    Math.min(request.leaderCommit(), lastLogIndexLocked()));
            stateMachine.restore(snapshot.values());
            lastApplied = snapshot.lastIncludedIndex();
            applyCommittedLocked();
            persistLocked();
            emitLocked("snapshot-installed", "index=" + snapshot.lastIncludedIndex());
            return new InstallSnapshot.Response(currentTerm, true, snapshot.lastIncludedIndex());
        }
    }

    public WriteResult put(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (key.isEmpty() || key.length() > MAX_KEY_CHARS) throw new IllegalArgumentException("invalid key length");
        if (value.length() > MAX_VALUE_CHARS) throw new IllegalArgumentException("value too large");

        final long index;
        final int writeTerm;
        synchronized (mutex) {
            ensureOpenLocked();
            if (role != Role.LEADER) return new WriteResult(ResultCode.NOT_LEADER, leaderId, -1);
            index = lastLogIndexLocked() + 1;
            writeTerm = currentTerm;
            log.add(LogEntry.put(index, currentTerm, key, value));
            matchIndex.put(id, index);
            persistLocked();
            emitLocked("client-put", "index=" + index + ", key=" + summarize(key));
        }

        ReplicationResult replication = replicateNow();
        synchronized (mutex) {
            if (role != Role.LEADER || currentTerm != writeTerm) {
                return new WriteResult(ResultCode.NOT_LEADER, leaderId, index);
            }
            if (commitIndex >= index && replication.hasQuorum()) {
                return new WriteResult(ResultCode.OK, id, index);
            }
            return new WriteResult(ResultCode.NO_QUORUM, id, index);
        }
    }

    /** Leader-only read guarded by a fresh quorum round and a committed entry in this term. */
    public ReadResult getLinearizable(String key) {
        Objects.requireNonNull(key, "key");
        synchronized (mutex) {
            ensureOpenLocked();
            if (role != Role.LEADER) return new ReadResult(ResultCode.NOT_LEADER, null, leaderId);
        }

        ReplicationResult result = replicateNow();
        synchronized (mutex) {
            if (!result.hasQuorum() || role != Role.LEADER || !hasCommittedCurrentTermLocked()) {
                return new ReadResult(ResultCode.NO_QUORUM, null, role == Role.LEADER ? id : leaderId);
            }
            String value = stateMachine.get(key);
            return new ReadResult(value == null ? ResultCode.NOT_FOUND : ResultCode.OK, value, id);
        }
    }

    /** Replicates all currently retained entries and propagates a newly advanced commit index. */
    public ReplicationResult replicateNow() {
        final int termAtStart;
        synchronized (mutex) {
            if (closed || role != Role.LEADER) return new ReplicationResult(0, clusterUrls.size(), false);
            termAtStart = currentTerm;
        }

        int acknowledgements = 1;
        for (Map.Entry<String, String> peer : peers.entrySet()) {
            if (replicatePeer(peer.getKey(), peer.getValue(), termAtStart)) acknowledgements++;
        }

        boolean commitAdvanced;
        synchronized (mutex) {
            if (role != Role.LEADER || currentTerm != termAtStart) {
                return new ReplicationResult(acknowledgements, clusterUrls.size(), false);
            }
            commitAdvanced = advanceCommitIndexLocked();
        }

        if (commitAdvanced) {
            for (Map.Entry<String, String> peer : peers.entrySet()) {
                replicatePeer(peer.getKey(), peer.getValue(), termAtStart);
            }
        }
        synchronized (mutex) {
            return new ReplicationResult(
                    acknowledgements,
                    clusterUrls.size(),
                    role == Role.LEADER && currentTerm == termAtStart);
        }
    }

    private boolean replicatePeer(String peerId, String url, int leaderTerm) {
        int maxAttempts;
        synchronized (mutex) {
            maxAttempts = Math.max(8, log.size() / Math.max(1, config.appendBatchSize()) + 8);
        }

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            final AppendEntries.Request appendRequest;
            final InstallSnapshot.Request snapshotRequest;
            synchronized (mutex) {
                if (closed || role != Role.LEADER || currentTerm != leaderTerm) return false;
                long next = nextIndex.getOrDefault(peerId, lastLogIndexLocked() + 1);
                if (next <= snapshot.lastIncludedIndex()) {
                    snapshotRequest = new InstallSnapshot.Request(
                            currentTerm,
                            id,
                            snapshot.lastIncludedIndex(),
                            snapshot.lastIncludedTerm(),
                            snapshot.values(),
                            commitIndex);
                    appendRequest = null;
                } else {
                    long previous = next - 1;
                    appendRequest = new AppendEntries.Request(
                            currentTerm,
                            id,
                            previous,
                            termAtLocked(previous),
                            entriesFromLocked(next, config.appendBatchSize()),
                            commitIndex);
                    snapshotRequest = null;
                }
            }

            if (snapshotRequest != null) {
                InstallSnapshot.Response response = transport.installSnapshot(peerId, url, snapshotRequest);
                if (response == null) return false;
                synchronized (mutex) {
                    if (response.term() > currentTerm) {
                        becomeFollowerLocked(response.term(), null, "higher-term snapshot response");
                        return false;
                    }
                    if (role != Role.LEADER || currentTerm != leaderTerm || !response.accepted()) return false;
                    matchIndex.put(peerId, response.matchIndex());
                    nextIndex.put(peerId, response.matchIndex() + 1);
                }
                continue;
            }

            AppendEntries.Response response = transport.appendEntries(peerId, url, appendRequest);
            if (response == null) return false;
            synchronized (mutex) {
                if (response.term() > currentTerm) {
                    becomeFollowerLocked(response.term(), null, "higher-term append response");
                    return false;
                }
                if (role != Role.LEADER || currentTerm != leaderTerm) return false;
                if (response.success()) {
                    long matched = Math.max(appendRequest.prevLogIndex(), response.matchIndex());
                    matchIndex.put(peerId, matched);
                    nextIndex.put(peerId, matched + 1);
                    if (matched >= lastLogIndexLocked()) return true;
                } else {
                    long newNext;
                    if (response.conflictTerm() >= 0) {
                        long lastForTerm = lastIndexOfTermLocked(response.conflictTerm());
                        newNext = lastForTerm >= 0 ? lastForTerm + 1 : response.conflictIndex();
                    } else {
                        newNext = response.conflictIndex();
                    }
                    nextIndex.put(peerId, Math.max(0, Math.min(newNext, lastLogIndexLocked() + 1)));
                }
            }
        }
        return false;
    }

    public Status status() {
        synchronized (mutex) {
            return new Status(
                    id,
                    role,
                    currentTerm,
                    leaderId,
                    commitIndex,
                    lastApplied,
                    lastLogIndexLocked(),
                    snapshot.lastIncludedIndex(),
                    log.size());
        }
    }

    public Optional<String> leaderUrl() {
        synchronized (mutex) {
            return Optional.ofNullable(leaderId).map(clusterUrls::get);
        }
    }

    public List<RaftTraceEvent> traceSnapshot() {
        synchronized (mutex) {
            return List.copyOf(trace);
        }
    }

    List<LogEntry> logSnapshotForTest() {
        synchronized (mutex) {
            return List.copyOf(log);
        }
    }

    private void becomeLeaderLocked() {
        role = Role.LEADER;
        leaderId = id;
        long beforeNoop = lastLogIndexLocked() + 1;
        for (String peerId : peers.keySet()) {
            nextIndex.put(peerId, beforeNoop);
            matchIndex.put(peerId, snapshot.lastIncludedIndex());
        }
        log.add(LogEntry.noop(beforeNoop, currentTerm));
        matchIndex.put(id, beforeNoop);
        nextHeartbeatMs = now();
        persistLocked();
        emitLocked("became-leader", "noopIndex=" + beforeNoop);
    }

    private void becomeFollowerLocked(int newTerm, String observedLeader, String reason) {
        boolean termChanged = newTerm > currentTerm;
        currentTerm = Math.max(currentTerm, newTerm);
        if (termChanged) votedFor = null;
        role = Role.FOLLOWER;
        leaderId = observedLeader;
        nextIndex.clear();
        matchIndex.clear();
        resetElectionDeadlineLocked(now());
        if (termChanged) persistLocked();
        emitLocked("became-follower", reason);
    }

    private boolean advanceCommitIndexLocked() {
        long previousCommit = commitIndex;
        matchIndex.put(id, lastLogIndexLocked());
        for (long candidate = lastLogIndexLocked(); candidate > commitIndex; candidate--) {
            if (termAtLocked(candidate) != currentTerm) continue;
            int replicated = 1;
            for (String peerId : peers.keySet()) {
                if (matchIndex.getOrDefault(peerId, snapshot.lastIncludedIndex()) >= candidate) replicated++;
            }
            if (hasMajority(replicated)) {
                commitIndex = candidate;
                break;
            }
        }
        if (commitIndex > previousCommit) {
            applyCommittedLocked();
            maybeCompactLocked();
            persistLocked();
            emitLocked("commit", "from=" + previousCommit + ", to=" + commitIndex);
            return true;
        }
        return false;
    }

    private void applyCommittedLocked() {
        while (lastApplied < commitIndex) {
            long next = lastApplied + 1;
            if (next <= snapshot.lastIncludedIndex()) {
                lastApplied = snapshot.lastIncludedIndex();
                continue;
            }
            LogEntry entry = entryAtLocked(next);
            if (entry.command() == LogEntry.Command.PUT) stateMachine.put(entry.key(), entry.value());
            lastApplied = next;
        }
    }

    private void restoreCommittedStateLocked() {
        stateMachine.restore(snapshot.values());
        lastApplied = snapshot.lastIncludedIndex();
        applyCommittedLocked();
    }

    private void maybeCompactLocked() {
        if (config.snapshotThreshold() == 0) return;
        if (commitIndex - snapshot.lastIncludedIndex() < config.snapshotThreshold()) return;
        int term = termAtLocked(commitIndex);
        Map<String, String> values = stateMachine.snapshot();
        log.removeIf(entry -> entry.index() <= commitIndex);
        snapshot = new RaftStorage.Snapshot(commitIndex, term, values);
        lastApplied = Math.max(lastApplied, snapshot.lastIncludedIndex());
        emitLocked("snapshot-created", "index=" + snapshot.lastIncludedIndex() + ", keys=" + values.size());
    }

    private AppendEntries.Response appendFailureLocked(long conflictIndex, int conflictTerm) {
        return new AppendEntries.Response(currentTerm, false, -1, conflictIndex, conflictTerm);
    }

    private void truncateFromLocked(long index) {
        int offset = Math.toIntExact(index - snapshot.lastIncludedIndex() - 1);
        while (log.size() > offset) log.remove(log.size() - 1);
    }

    private List<LogEntry> entriesFromLocked(long index, int limit) {
        if (index > lastLogIndexLocked()) return List.of();
        int start = Math.toIntExact(index - snapshot.lastIncludedIndex() - 1);
        int end = Math.min(log.size(), start + limit);
        return List.copyOf(log.subList(start, end));
    }

    private List<LogEntry> entriesAfterLocked(long index) {
        if (index >= lastLogIndexLocked()) return List.of();
        int start = Math.toIntExact(index - snapshot.lastIncludedIndex());
        return List.copyOf(log.subList(start, log.size()));
    }

    private LogEntry entryAtLocked(long index) {
        int offset = Math.toIntExact(index - snapshot.lastIncludedIndex() - 1);
        if (offset < 0 || offset >= log.size()) throw new IllegalStateException("missing log index " + index);
        return log.get(offset);
    }

    private int termAtLocked(long index) {
        if (index == -1) return -1;
        if (index == snapshot.lastIncludedIndex()) return snapshot.lastIncludedTerm();
        if (index < snapshot.lastIncludedIndex() || index > lastLogIndexLocked()) return -1;
        return entryAtLocked(index).term();
    }

    private long firstIndexOfTermLocked(int term) {
        if (snapshot.lastIncludedTerm() == term) return snapshot.lastIncludedIndex();
        for (LogEntry entry : log) if (entry.term() == term) return entry.index();
        return lastLogIndexLocked() + 1;
    }

    private long lastIndexOfTermLocked(int term) {
        for (int i = log.size() - 1; i >= 0; i--) if (log.get(i).term() == term) return log.get(i).index();
        return snapshot.lastIncludedTerm() == term ? snapshot.lastIncludedIndex() : -1;
    }

    private long lastLogIndexLocked() {
        return log.isEmpty() ? snapshot.lastIncludedIndex() : log.get(log.size() - 1).index();
    }

    private int lastLogTermLocked() {
        return log.isEmpty() ? snapshot.lastIncludedTerm() : log.get(log.size() - 1).term();
    }

    private boolean hasCommittedCurrentTermLocked() {
        return commitIndex >= 0 && termAtLocked(commitIndex) == currentTerm;
    }

    private boolean hasMajority(int count) {
        return count > clusterUrls.size() / 2;
    }

    private void resetElectionDeadlineLocked(long nowMs) {
        long spread = config.electionMaxMs() - config.electionMinMs() + 1;
        electionDeadlineMs = nowMs + config.electionMinMs() + random.nextLong(spread);
    }

    private RaftStorage.StoredState loadState() {
        try {
            return storage.load();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot load Raft state", e);
        }
    }

    private void persistLocked() {
        try {
            storage.save(new RaftStorage.StoredState(currentTerm, votedFor, commitIndex, snapshot, log));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot persist Raft state", e);
        }
    }

    private void emitLocked(String event, String detail) {
        trace.addLast(new RaftTraceEvent(
                ++traceSequence,
                now(),
                id,
                currentTerm,
                role.name(),
                event,
                detail));
        while (trace.size() > config.traceCapacity()) trace.removeFirst();
    }

    private void ensureOpenLocked() {
        if (closed) throw new IllegalStateException("Raft node is closed");
    }

    private long now() {
        return clock.millis();
    }

    private static String requireNodeId(String value) {
        Objects.requireNonNull(value, "id");
        if (!value.matches("[A-Za-z0-9._-]{1,64}")) throw new IllegalArgumentException("invalid node id");
        return value;
    }

    private static String summarize(String value) {
        return value.length() <= 48 ? value : value.substring(0, 45) + "...";
    }

    @Override
    public void close() {
        ScheduledExecutorService toStop;
        synchronized (mutex) {
            if (closed) return;
            closed = true;
            toStop = scheduler;
            scheduler = null;
            emitLocked("closed", "node stopped");
        }
        if (toStop != null) toStop.shutdownNow();
        if (stateMachine instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                throw new RuntimeException("cannot close state machine", e);
            }
        }
    }
}
