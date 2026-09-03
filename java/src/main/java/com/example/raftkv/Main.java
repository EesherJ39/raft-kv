package com.example.raftkv;

import java.net.URI;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** Starts one process in a RaftKV cluster from environment configuration. */
public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        String nodeId = env("NODE_ID", "node1");
        int port = parseInt("PORT", env("PORT", "8080"), 1, 65_535);
        String dataDir = env("DATA_DIR", "data/" + nodeId);
        String defaultPeers = nodeId + "=http://127.0.0.1:" + port;
        Map<String, String> cluster = parsePeers(env("PEERS", defaultPeers));
        if (!cluster.containsKey(nodeId)) {
            throw new IllegalArgumentException("PEERS must include NODE_ID " + nodeId);
        }

        int snapshotThreshold = parseInt(
                "RAFT_SNAPSHOT_THRESHOLD",
                env("RAFT_SNAPSHOT_THRESHOLD", "256"),
                0,
                1_000_000);
        RaftConfig defaults = RaftConfig.production();
        RaftConfig config = new RaftConfig(
                defaults.electionMinMs(),
                defaults.electionMaxMs(),
                defaults.heartbeatMs(),
                defaults.appendBatchSize(),
                snapshotThreshold,
                defaults.traceCapacity());

        Storage stateMachine = new Storage(dataDir);
        FileRaftStorage consensusStorage = new FileRaftStorage(Path.of(dataDir, "raft-state.bin"));
        RaftNode node = new RaftNode(
                nodeId,
                cluster,
                new HttpPeerClient(),
                stateMachine,
                consensusStorage,
                config);
        KeyValueService service = new KeyValueService(node, port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            service.close();
            node.close();
        }, "raft-shutdown"));

        service.start();
        node.start();
        System.out.println("RaftKV node " + nodeId + " listening on port " + port + " with peers " + cluster.keySet());
        new CountDownLatch(1).await();
    }

    static Map<String, String> parsePeers(String value) {
        LinkedHashMap<String, String> peers = new LinkedHashMap<>();
        for (String part : value.split(",")) {
            String[] pair = part.trim().split("=", 2);
            if (pair.length != 2 || !pair[0].matches("[A-Za-z0-9._-]{1,64}")) {
                throw new IllegalArgumentException("invalid PEERS entry: " + part);
            }
            URI uri = URI.create(pair[1]);
            if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) || uri.getHost() == null) {
                throw new IllegalArgumentException("peer URL must be HTTP(S): " + pair[1]);
            }
            String base = pair[1].endsWith("/") ? pair[1].substring(0, pair[1].length() - 1) : pair[1];
            if (peers.putIfAbsent(pair[0], base) != null) {
                throw new IllegalArgumentException("duplicate peer id: " + pair[0]);
            }
        }
        if (peers.isEmpty() || peers.size() % 2 == 0) {
            throw new IllegalArgumentException("RaftKV requires a non-empty odd-sized cluster");
        }
        return peers;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static int parseInt(String name, String value, int min, int max) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < min || parsed > max) throw new IllegalArgumentException();
            return parsed;
        } catch (IllegalArgumentException error) {
            throw new IllegalArgumentException(name + " must be between " + min + " and " + max);
        }
    }
}
