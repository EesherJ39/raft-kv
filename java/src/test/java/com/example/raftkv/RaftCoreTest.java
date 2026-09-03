package com.example.raftkv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** Dependency-free protocol regression suite; run with assertions enabled. */
public final class RaftCoreTest {
    private int passed;

    public static void main(String[] args) throws Exception {
        RaftCoreTest suite = new RaftCoreTest();
        suite.run("vote survives restart", suite::voteSurvivesRestart);
        suite.run("election, replication, and linearizable read", suite::electionReplicationAndRead);
        suite.run("quorum loss rejects writes and reads", suite::quorumLossRejectsOperations);
        suite.run("conflicting suffix is repaired", suite::conflictingSuffixIsRepaired);
        suite.run("heartbeat does not acknowledge an unmatched suffix", suite::heartbeatDoesNotAcknowledgeUnmatchedSuffix);
        suite.run("snapshot catches up a restarted follower", suite::snapshotCatchesUpFollower);
        suite.run("file state recovers and detects corruption", suite::fileStateRecoveryAndChecksum);
        suite.run("binary RPC codec round trips", suite::rpcCodecRoundTrips);
        System.out.println("RAFT CORE TESTS PASS: " + suite.passed + "/8");
    }

    private void voteSurvivesRestart() {
        Map<String, String> cluster = Map.of("A", "memory://A");
        InMemoryRaftStorage store = new InMemoryRaftStorage();
        PeerClient noPeers = unreachableTransport();
        RaftNode first = new RaftNode(
                "A", cluster, noPeers, new InMemoryStateMachine(), store, RaftConfig.deterministicTests(0));
        assertTrue(first.onRequestVote(new RequestVote.Request(7, "candidate-one", -1, -1)).voteGranted());
        first.close();

        RaftNode restarted = new RaftNode(
                "A", cluster, noPeers, new InMemoryStateMachine(), store, RaftConfig.deterministicTests(0));
        assertFalse(restarted.onRequestVote(new RequestVote.Request(7, "candidate-two", -1, -1)).voteGranted());
        assertEquals(7, restarted.status().term());
        restarted.close();
    }

    private void electionReplicationAndRead() {
        try (TestCluster cluster = new TestCluster(List.of("A", "B", "C"), 0)) {
            assertTrue(cluster.elect("A"));
            RaftNode.WriteResult write = cluster.node("A").put("language", "java");
            assertEquals(RaftNode.ResultCode.OK, write.code());
            cluster.replicate("A", 2);
            for (String id : cluster.ids()) assertEquals("java", cluster.machine(id).get("language"));
            RaftNode.ReadResult read = cluster.node("A").getLinearizable("language");
            assertEquals(RaftNode.ResultCode.OK, read.code());
            assertEquals("java", read.value());
        }
    }

    private void quorumLossRejectsOperations() {
        try (TestCluster cluster = new TestCluster(List.of("A", "B", "C"), 0)) {
            assertTrue(cluster.elect("A"));
            cluster.isolate("A");
            assertEquals(RaftNode.ResultCode.NO_QUORUM, cluster.node("A").put("unsafe", "value").code());
            assertEquals(RaftNode.ResultCode.NO_QUORUM, cluster.node("A").getLinearizable("unsafe").code());
            assertEquals(null, cluster.machine("B").get("unsafe"));
            assertEquals(null, cluster.machine("C").get("unsafe"));
        }
    }

    private void conflictingSuffixIsRepaired() {
        try (TestCluster cluster = new TestCluster(List.of("A", "B", "C"), 0)) {
            assertTrue(cluster.elect("A"));
            assertEquals(RaftNode.ResultCode.OK, cluster.node("A").put("stable", "one").code());
            cluster.isolate("A");
            assertEquals(RaftNode.ResultCode.NO_QUORUM, cluster.node("A").put("winner", "stale").code());

            assertTrue(cluster.elect("B"));
            assertEquals(RaftNode.ResultCode.OK, cluster.node("B").put("winner", "fresh").code());
            cluster.heal();
            cluster.replicate("B", 4);

            for (String id : cluster.ids()) {
                assertEquals("one", cluster.machine(id).get("stable"));
                assertEquals("fresh", cluster.machine(id).get("winner"));
            }
            assertEquals(RaftNode.Role.FOLLOWER, cluster.node("A").status().role());
        }
    }

    private void heartbeatDoesNotAcknowledgeUnmatchedSuffix() {
        Map<String, String> cluster = Map.of("B", "memory://B");
        RaftNode follower = new RaftNode(
                "B",
                cluster,
                unreachableTransport(),
                new InMemoryStateMachine(),
                new InMemoryRaftStorage(),
                RaftConfig.deterministicTests(0));
        try {
            AppendEntries.Response seeded = follower.onAppendEntries(new AppendEntries.Request(
                    1,
                    "A",
                    -1,
                    -1,
                    List.of(LogEntry.noop(0, 1), LogEntry.put(1, 1, "stale", "tail")),
                    0));
            assertTrue(seeded.success());
            assertEquals(1L, seeded.matchIndex());

            AppendEntries.Response heartbeat = follower.onAppendEntries(new AppendEntries.Request(
                    2, "C", 0, 1, List.of(), 0));
            assertTrue(heartbeat.success());
            assertEquals(0L, heartbeat.matchIndex());
            assertEquals(1L, follower.status().lastLogIndex());
        } finally {
            follower.close();
        }
    }

    private void snapshotCatchesUpFollower() {
        try (TestCluster cluster = new TestCluster(List.of("A", "B", "C"), 4)) {
            assertTrue(cluster.elect("A"));
            cluster.crash("C");
            for (int i = 0; i < 15; i++) {
                assertEquals(RaftNode.ResultCode.OK, cluster.node("A").put("key-" + i, "value-" + i).code());
            }
            assertTrue(cluster.node("A").status().snapshotIndex() >= 11);

            cluster.restart("C");
            cluster.replicate("A", 5);
            for (int i = 0; i < 15; i++) assertEquals("value-" + i, cluster.machine("C").get("key-" + i));
            assertTrue(cluster.node("C").status().snapshotIndex() >= 0);
        }
    }

    private void fileStateRecoveryAndChecksum() throws Exception {
        Path directory = Files.createTempDirectory("raftkv-file-state-");
        Path statePath = directory.resolve("raft-state.bin");
        try {
            Map<String, String> cluster = Map.of("A", "memory://A");
            RaftNode first = new RaftNode(
                    "A",
                    cluster,
                    unreachableTransport(),
                    new InMemoryStateMachine(),
                    new FileRaftStorage(statePath),
                    RaftConfig.deterministicTests(2));
            assertTrue(first.campaign());
            assertEquals(RaftNode.ResultCode.OK, first.put("durable", "yes").code());
            first.close();

            InMemoryStateMachine recoveredMachine = new InMemoryStateMachine();
            RaftNode recovered = new RaftNode(
                    "A",
                    cluster,
                    unreachableTransport(),
                    recoveredMachine,
                    new FileRaftStorage(statePath),
                    RaftConfig.deterministicTests(2));
            assertEquals("yes", recoveredMachine.get("durable"));
            recovered.close();

            byte[] corrupted = Files.readAllBytes(statePath);
            corrupted[corrupted.length - 1] ^= 0x01;
            Files.write(statePath, corrupted);
            expectThrows(IOException.class, () -> new FileRaftStorage(statePath).load());
        } finally {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
            }
        }
    }

    private void rpcCodecRoundTrips() throws IOException {
        LogEntry entry = LogEntry.put(9, 4, "alpha", "beta");
        AppendEntries.Request append = new AppendEntries.Request(4, "A", 8, 3, List.of(entry), 7);
        assertEquals(append, RpcCodec.decodeAppendEntries(RpcCodec.encodeAppendEntries(append)));

        RequestVote.Request vote = new RequestVote.Request(5, "B", 11, 5);
        assertEquals(vote, RpcCodec.decodeRequestVote(RpcCodec.encodeRequestVote(vote)));

        InstallSnapshot.Request snapshot = new InstallSnapshot.Request(6, "C", 20, 6, Map.of("k", "v"), 20);
        assertEquals(snapshot, RpcCodec.decodeInstallSnapshot(RpcCodec.encodeInstallSnapshot(snapshot)));
    }

    private void run(String name, CheckedRunnable test) throws Exception {
        test.run();
        passed++;
        System.out.println("PASS " + name);
    }

    private static PeerClient unreachableTransport() {
        return new PeerClient() {
            public RequestVote.Response requestVote(String peerId, String baseUrl, RequestVote.Request request) { return null; }
            public AppendEntries.Response appendEntries(String peerId, String baseUrl, AppendEntries.Request request) { return null; }
            public InstallSnapshot.Response installSnapshot(String peerId, String baseUrl, InstallSnapshot.Request request) { return null; }
        };
    }

    private static void assertTrue(boolean value) {
        if (!value) throw new AssertionError("expected true");
    }

    private static void assertFalse(boolean value) {
        if (value) throw new AssertionError("expected false");
    }

    private static void assertEquals(Object expected, Object actual) {
        if (!java.util.Objects.equals(expected, actual)) {
            throw new AssertionError("expected " + expected + " but got " + actual);
        }
    }

    private static void expectThrows(Class<? extends Throwable> type, CheckedRunnable action) throws Exception {
        try {
            action.run();
        } catch (Throwable error) {
            if (type.isInstance(error)) return;
            throw new AssertionError("expected " + type.getSimpleName() + " but got " + error, error);
        }
        throw new AssertionError("expected " + type.getSimpleName());
    }

    @FunctionalInterface
    private interface CheckedRunnable { void run() throws Exception; }
}
