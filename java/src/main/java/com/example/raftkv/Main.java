package com.example.raftkv;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Bootstraps one Raft node from env:
 *   NODE_ID   : "node1"
 *   PORT      : "8080"
 *   DATA_DIR  : "/data"
 *   PEERS     : "node1=http://raftkv-node1:8080,node2=http://raftkv-node2:8080,node3=http://raftkv-node3:8080"
 *
 * Starts the HTTP service and then blocks forever.
 */
public class Main {

    private static Map<String,String> parsePeers(String peersEnv) {
        Map<String,String> m = new LinkedHashMap<>();
        if (peersEnv == null || peersEnv.isEmpty()) return m;
        for (String part : peersEnv.split(",")) {
            String[] kv = part.split("=", 2);
            if (kv.length == 2) {
                m.put(kv[0].trim(), kv[1].trim());
            }
        }
        return m;
    }

    public static void main(String[] args) throws Exception {
        String nodeId = System.getenv().getOrDefault("NODE_ID", "node1");
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        String dataDir = System.getenv().getOrDefault("DATA_DIR", "/data");
        Map<String,String> peers = parsePeers(System.getenv("PEERS"));

        // JNI-backed KV store (already provided elsewhere in the project)
        Storage storage = new Storage(dataDir);

        // Simple HTTP peer client for Raft RPCs (already in project)
        PeerClient rpc = new HttpPeerClient();

        // Raft core
        RaftNode raft = new RaftNode(nodeId, peers, storage, rpc);

        // Start HTTP endpoints (status, get/put, requestVote, append)
        KeyValueService http = new KeyValueService(raft, port);
        http.serve();

        // Keep the JVM alive
        new CountDownLatch(1).await();
    }
}