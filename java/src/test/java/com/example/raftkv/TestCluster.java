package com.example.raftkv;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Deterministic, in-process transport with partitions, crashes, and duplicate delivery. */
final class TestCluster implements AutoCloseable {
    private final List<String> ids;
    private final Map<String, String> urls = new LinkedHashMap<>();
    private final Map<String, InMemoryRaftStorage> stores = new LinkedHashMap<>();
    private final Map<String, InMemoryStateMachine> machines = new LinkedHashMap<>();
    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final FaultNetwork network = new FaultNetwork();
    private final RaftConfig config;

    TestCluster(List<String> ids, int snapshotThreshold) {
        if (ids.size() % 2 == 0) throw new IllegalArgumentException("test cluster size must be odd");
        this.ids = List.copyOf(ids);
        this.config = RaftConfig.deterministicTests(snapshotThreshold);
        for (String id : ids) {
            urls.put(id, "memory://" + id);
            stores.put(id, new InMemoryRaftStorage());
        }
        for (String id : ids) restart(id);
    }

    RaftNode node(String id) {
        RaftNode node = nodes.get(id);
        if (node == null || network.crashed.contains(id)) throw new IllegalStateException("node unavailable: " + id);
        return node;
    }

    InMemoryStateMachine machine(String id) {
        return machines.get(id);
    }

    List<String> ids() {
        return ids;
    }

    boolean elect(String id) {
        return node(id).campaign();
    }

    void partition(String left, String right) {
        network.blocked.add(link(left, right));
        network.blocked.add(link(right, left));
    }

    void isolate(String id) {
        for (String other : ids) if (!id.equals(other)) partition(id, other);
    }

    void heal() {
        network.blocked.clear();
    }

    void crash(String id) {
        network.crashed.add(id);
        RaftNode node = nodes.get(id);
        if (node != null) node.close();
    }

    void restart(String id) {
        RaftNode previous = nodes.get(id);
        if (previous != null && !network.crashed.contains(id)) previous.close();
        InMemoryStateMachine machine = new InMemoryStateMachine();
        RaftNode node = new RaftNode(
                id,
                urls,
                network.clientFor(id),
                machine,
                stores.get(id),
                config);
        machines.put(id, machine);
        nodes.put(id, node);
        network.nodes.put(id, node);
        network.crashed.remove(id);
    }

    void replicate(String leader, int rounds) {
        for (int i = 0; i < rounds; i++) node(leader).replicateNow();
    }

    Map<String, String> values(String id) {
        return machine(id).snapshot();
    }

    void duplicateEvery(int every) {
        network.duplicateEvery = every;
    }

    long blockedRpcCount() {
        return network.blockedRpcCount;
    }

    long duplicateDeliveryCount() {
        return network.duplicateDeliveryCount;
    }

    private static String link(String source, String target) {
        return source + "->" + target;
    }

    @Override
    public void close() {
        Set<RaftNode> unique = new LinkedHashSet<>(nodes.values());
        for (RaftNode node : unique) {
            try {
                node.close();
            } catch (RuntimeException ignored) {
                // Tests should report their primary assertion, not duplicate-close noise.
            }
        }
    }

    private final class FaultNetwork {
        private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
        private final Set<String> blocked = new LinkedHashSet<>();
        private final Set<String> crashed = new LinkedHashSet<>();
        private long calls;
        private long blockedRpcCount;
        private long duplicateDeliveryCount;
        private int duplicateEvery;

        PeerClient clientFor(String source) {
            return new PeerClient() {
                @Override
                public RequestVote.Response requestVote(
                        String peerId, String baseUrl, RequestVote.Request request) {
                    RaftNode target = target(source, peerId);
                    return target == null ? null : target.onRequestVote(request);
                }

                @Override
                public AppendEntries.Response appendEntries(
                        String peerId, String baseUrl, AppendEntries.Request request) {
                    RaftNode target = target(source, peerId);
                    if (target == null) return null;
                    AppendEntries.Response response = target.onAppendEntries(request);
                    if (shouldDuplicate()) {
                        target.onAppendEntries(request);
                        duplicateDeliveryCount++;
                    }
                    return response;
                }

                @Override
                public InstallSnapshot.Response installSnapshot(
                        String peerId, String baseUrl, InstallSnapshot.Request request) {
                    RaftNode target = target(source, peerId);
                    if (target == null) return null;
                    InstallSnapshot.Response response = target.onInstallSnapshot(request);
                    if (shouldDuplicate()) {
                        target.onInstallSnapshot(request);
                        duplicateDeliveryCount++;
                    }
                    return response;
                }
            };
        }

        private RaftNode target(String source, String target) {
            calls++;
            if (crashed.contains(source) || crashed.contains(target) || blocked.contains(link(source, target))) {
                blockedRpcCount++;
                return null;
            }
            return nodes.get(target);
        }

        private boolean shouldDuplicate() {
            return duplicateEvery > 0 && calls % duplicateEvery == 0;
        }
    }
}
